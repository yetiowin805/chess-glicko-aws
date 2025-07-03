import os
import json
import argparse
import asyncio
import aiohttp
import aiofiles
import boto3
import logging
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path
from typing import Set, List
from botocore.exceptions import ClientError
from countries import countries

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_AWS_REGION = "us-east-2"

class FIDETournamentScraper:
    def __init__(self, s3_bucket, aws_region=DEFAULT_AWS_REGION):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3', region_name=aws_region)
        self.local_temp_dir = "/tmp/chess_data"
        
        # Async configuration
        self.max_concurrent_requests = 20  # Increased for cloud environment
        self.semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        
    def is_valid_rating_period(self, year: int, month: int) -> bool:
        """
        Validate if the given year and month combination is a valid FIDE rating period.
        """
        if year < 2009:
            return month % 3 == 1
        if year == 2009:
            return month < 7 and month % 3 == 1 or month >= 7 and month % 2 == 1
        if year < 2012:
            return month % 2 == 1
        if year == 2012:
            return (month < 7 and month % 2 == 1) or month >= 7
        return True

    async def scrape_tournaments_for_month(self, month_str):
        """Scrape tournament data for all countries for a specific month"""
        try:
            year, month = map(int, month_str.split("-"))
            logger.info(f"Starting tournament scraping for {year}-{month:02d}")
            
            if not self.is_valid_rating_period(year, month):
                logger.info(f"Invalid rating period: {year}-{month:02d}, skipping")
                return True
            
            # Setup local temp directory
            os.makedirs(self.local_temp_dir, exist_ok=True)
            
            # Configure async session with optimized settings for cloud
            connector = aiohttp.TCPConnector(
                limit=100,
                limit_per_host=10,
                keepalive_timeout=30,
                enable_cleanup_closed=True
            )
            timeout = aiohttp.ClientTimeout(total=60, connect=10)
            
            async with aiohttp.ClientSession(
                connector=connector, 
                timeout=timeout,
                headers={'User-Agent': 'FIDE Tournament Scraper 1.0'}
            ) as session:
                
                # Create tasks for all countries
                tasks = [
                    self._scrape_country_tournaments(session, country, year, month)
                    for country in countries
                ]
                
                # Execute all tasks concurrently
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results and log summary
                successful = sum(1 for r in results if r is True)
                failed = sum(1 for r in results if isinstance(r, Exception))
                skipped = len(results) - successful - failed
                
                logger.info(f"Tournament scraping completed: {successful} successful, {failed} failed, {skipped} skipped")
                
                # Upload completion marker
                await self._upload_completion_marker(month_str, successful, failed, skipped)
                
                return True
                
        except Exception as e:
            logger.error(f"Error in tournament scraping: {str(e)}")
            raise

    async def _scrape_country_tournaments(self, session: aiohttp.ClientSession, country: str, year: int, month: int):
        """Scrape tournament data for a specific country"""
        month_str = f"{month:02d}"
        s3_key = f"persistent/tournament_data/raw/{country}/{year}-{month_str}/tournaments.txt"
        
        # Check if data already exists in S3
        if await self._check_s3_file_exists_async(s3_key):
            logger.debug(f"Tournament data already exists for {country} {year}-{month_str}")
            return True
        
        try:
            async with self.semaphore:
                # Make API request to FIDE
                api_url = "https://ratings.fide.com/a_tournaments.php"
                params = {
                    "country": country,
                    "period": f"{year}-{month_str}-01"
                }
                
                headers = {
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": f"https://ratings.fide.com/rated_tournaments.phtml?country={country}&period={year}-{month_str}-01"
                }
                
                async with session.get(api_url, params=params, headers=headers) as response:
                    response.raise_for_status()
                    
                    content = await response.text()
                    if not content:
                        logger.warning(f"Empty response for {country} {year}-{month_str}")
                        return True
                    
                    # Process the response
                    tournament_ids = await self._extract_tournament_ids(content, country, year, month)
                    
                    if tournament_ids:
                        # Save to local file
                        local_file = await self._save_tournaments_locally(tournament_ids, country, year, month)
                        
                        # Upload to S3
                        await self._upload_to_s3_async(local_file, s3_key)
                        
                        # Cleanup local file
                        os.remove(local_file)
                        
                        logger.info(f"Saved {len(tournament_ids)} tournaments for {country} {year}-{month_str}")
                    else:
                        logger.debug(f"No tournaments found for {country} {year}-{month_str}")
                    
                    return True
                    
        except aiohttp.ClientError as e:
            logger.error(f"HTTP error for {country} {year}-{month_str}: {str(e)}")
            return e
        except asyncio.TimeoutError as e:
            logger.error(f"Timeout for {country} {year}-{month_str}")
            return e
        except Exception as e:
            logger.error(f"Unexpected error for {country} {year}-{month_str}: {str(e)}")
            return e

    async def _extract_tournament_ids(self, content: str, country: str, year: int, month: int) -> List[str]:
        """Extract tournament IDs from FIDE API response"""
        try:
            # Clean up HTML entities
            content = content.replace("</a>", "")
            content = content.replace("&lt;", "<")
            content = content.replace("&gt;", ">")
            
            # Parse JSON
            data = json.loads(content)
            tournament_ids = []
            
            # Extract tournament IDs from the data
            if "data" in data and isinstance(data["data"], list):
                for tournament in data["data"]:
                    if isinstance(tournament, list) and len(tournament) > 0:
                        tournament_ids.append(str(tournament[0]))
            
            return tournament_ids
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error for {country} {year}-{month}: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Error extracting tournament IDs for {country} {year}-{month}: {str(e)}")
            return []

    async def _save_tournaments_locally(self, tournament_ids: List[str], country: str, year: int, month: int) -> str:
        """Save tournament IDs to local file"""
        month_str = f"{month:02d}"
        local_dir = os.path.join(self.local_temp_dir, "tournaments", country, f"{year}-{month_str}")
        os.makedirs(local_dir, exist_ok=True)
        
        local_file = os.path.join(local_dir, "tournaments.txt")
        
        async with aiofiles.open(local_file, "w") as f:
            await f.write("\n".join(tournament_ids))
        
        return local_file

    async def _check_s3_file_exists_async(self, s3_key: str) -> bool:
        """Async wrapper for checking S3 file existence"""
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, self.s3_client.head_object, self.s3_bucket, s3_key)
            return True
        except ClientError:
            return False

    async def _upload_to_s3_async(self, local_path: str, s3_key: str):
        """Async wrapper for S3 upload"""
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, self.s3_client.upload_file, local_path, self.s3_bucket, s3_key)
            logger.debug(f"Uploaded {local_path} to S3 as {s3_key}")
        except ClientError as e:
            logger.error(f"Failed to upload {local_path} to S3: {str(e)}")
            raise

    async def _upload_completion_marker(self, month_str: str, successful: int, failed: int, skipped: int):
        """Upload completion marker with statistics"""
        try:
            completion_data = {
                "month": month_str,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "statistics": {
                    "countries_processed": len(countries),
                    "successful": successful,
                    "failed": failed,
                    "skipped": skipped
                },
                "step": "tournament_scraping"
            }
            
            local_file = os.path.join(self.local_temp_dir, f"tournament_completion_{month_str}.json")
            async with aiofiles.open(local_file, "w") as f:
                await f.write(json.dumps(completion_data, indent=2))
            
            s3_key = f"results/{month_str}/tournament_scraping_completion.json"
            await self._upload_to_s3_async(local_file, s3_key)
            
            os.remove(local_file)
            logger.info(f"Uploaded completion marker for {month_str}")
            
        except Exception as e:
            logger.warning(f"Failed to upload completion marker: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Scrape FIDE tournament data.")
    parser.add_argument(
        "--month",
        type=str,
        help="Month for scraping in YYYY-MM format",
        required=True,
    )
    parser.add_argument(
        "--s3_bucket",
        type=str,
        help="S3 bucket for data storage",
        default=os.environ.get("S3_BUCKET"),
        required=True,
    )
    parser.add_argument(
        "--aws_region",
        type=str,
        help="AWS region",
        default=os.environ.get("AWS_REGION", DEFAULT_AWS_REGION),
    )

    args = parser.parse_args()
    
    # Validate month format
    try:
        datetime.strptime(args.month, "%Y-%m")
    except ValueError:
        logger.error("Month must be in YYYY-MM format")
        return 1
    
    # Initialize scraper
    scraper = FIDETournamentScraper(
        s3_bucket=args.s3_bucket,
        aws_region=args.aws_region
    )
    
    # Run scraping
    try:
        success = asyncio.run(scraper.scrape_tournaments_for_month(args.month))
        
        if success:
            logger.info(f"Tournament scraping completed successfully for {args.month}")
            return 0
        else:
            logger.error("Tournament scraping failed")
            return 1
            
    except Exception as e:
        logger.error(f"Tournament scraping failed with error: {str(e)}")
        return 1

if __name__ == "__main__":
    exit(main())