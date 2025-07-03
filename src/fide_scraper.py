import os
import json
import argparse
import asyncio
import aiohttp
import aiofiles
import boto3
import logging
from datetime import datetime
from pathlib import Path
from typing import List
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
        self.max_concurrent_requests = 5
        self.semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        
        logger.info(f"Initialized scraper - S3 bucket: {s3_bucket}, Region: {aws_region}")
        logger.info(f"Total countries to process: {len(countries)}")
        
    def is_valid_rating_period(self, year: int, month: int) -> bool:
        """
        Validate if the given year and month combination is a valid FIDE rating period.
        """
        valid = True
        if year < 2009:
            valid = month % 3 == 1
        elif year == 2009:
            valid = month < 7 and month % 3 == 1 or month >= 7 and month % 2 == 1
        elif year < 2012:
            valid = month % 2 == 1
        elif year == 2012:
            valid = (month < 7 and month % 2 == 1) or month >= 7
        
        return valid

    async def scrape_tournaments_for_month(self, month_str):
        """Scrape tournament data for all countries for a specific month"""
        try:
            year, month = map(int, month_str.split("-"))
            logger.info(f"Starting tournament scraping for {year}-{month:02d}")
            
            if not self.is_valid_rating_period(year, month):
                logger.warning(f"Invalid rating period: {year}-{month:02d}, skipping")
                return True
            
            # Setup local temp directory
            os.makedirs(self.local_temp_dir, exist_ok=True)
            
            # Configure async session
            connector = aiohttp.TCPConnector(limit=100)
            timeout = aiohttp.ClientTimeout(total=60)
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                # Create tasks for all countries
                tasks = [
                    self._scrape_country_tournaments(session, country, year, month)
                    for country in countries
                ]
                
                logger.info(f"Processing {len(tasks)} countries")
                
                # Execute all tasks concurrently
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results and log summary
                successful = []
                failed = []
                skipped = []
                
                for i, result in enumerate(results):
                    country = countries[i]
                    if result is True:
                        successful.append(country)
                    elif result is None:
                        skipped.append(country)
                    elif isinstance(result, Exception):
                        failed.append((country, str(result)))
                
                logger.info(f"Results - Successful: {len(successful)}, Skipped: {len(skipped)}, Failed: {len(failed)}")
                
                # Log failures for debugging
                if failed:
                    logger.error(f"Failed countries: {[f[0] for f in failed[:10]]}")
                    for country, error in failed[:3]:  # Log first 3 failure details
                        logger.error(f"  {country}: {error}")
                
                # Upload completion marker
                await self._upload_completion_marker(month_str, len(successful), len(failed), len(skipped))
                
                return True
                
        except Exception as e:
            logger.error(f"Error in tournament scraping: {str(e)}", exc_info=True)
            raise

    async def _scrape_country_tournaments(self, session: aiohttp.ClientSession, country: str, year: int, month: int):
        """Scrape tournament data for a specific country"""
        month_str = f"{month:02d}"
        
        if not self.is_valid_rating_period(year, month):
            return None
            
        s3_key = f"persistent/tournament_data/raw/{country}/{year}-{month_str}/tournaments.txt"
        
        # Check if data already exists in S3
        if await self._check_s3_file_exists_async(s3_key):
            return None  # Skipped
        
        api_url = "https://ratings.fide.com/a_tournaments.php"
        params = {
            "country": country,
            "period": f"{year}-{month_str}-01"
        }
        
        try:
            async with self.semaphore:
                headers = {
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": f"https://ratings.fide.com/rated_tournaments.phtml?country={country}&period={year}-{month_str}-01"
                }
                
                async with session.get(api_url, params=params, headers=headers, timeout=30) as api_response:
                    api_response.raise_for_status()
                    
                    raw_content = await api_response.read()
                    if not raw_content:
                        logger.warning(f"Empty response for {country}")
                        return Exception("Empty response")
                    
                    content = await api_response.text()
                    
        except aiohttp.ClientError as e:
            logger.warning(f"HTTP error for {country}: {str(e)}")
            return e
        except asyncio.TimeoutError:
            logger.warning(f"Timeout for {country}")
            return Exception("Timeout")
        except Exception as e:
            logger.error(f"Unexpected error for {country}: {str(e)}")
            return e

        # Process response content
        try:
            # Clean content
            content = content.replace("</a>", "")
            content = content.replace("&lt;", "<")
            content = content.replace("&gt;", ">")

            try:
                data = json.loads(content)
            except json.JSONDecodeError as je:
                logger.error(f"JSON decode failed for {country}: {str(je)}")
                return je

            tournament_ids = []

            # Extract tournament IDs from the data
            if "data" in data and isinstance(data["data"], list):
                for tournament in data["data"]:
                    if isinstance(tournament, list) and len(tournament) > 0:
                        tournament_ids.append(str(tournament[0]))
                        
            elif "aaData" in data and isinstance(data["aaData"], list):
                for tournament in data["aaData"]:
                    if isinstance(tournament, list) and len(tournament) > 0:
                        tournament_ids.append(str(tournament[0]))
            else:
                logger.info(f"No tournaments found for {country}")
                return True

            if tournament_ids:
                logger.info(f"Found {len(tournament_ids)} tournaments for {country}")
                
                # Save to local file
                local_file = await self._save_tournaments_locally(tournament_ids, country, year, month)
                
                # Upload to S3
                await self._upload_to_s3_async(local_file, s3_key)
                
                # Cleanup local file
                os.remove(local_file)
            else:
                logger.info(f"No tournaments found for {country}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing response for {country}: {str(e)}")
            return e

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
            await loop.run_in_executor(
                None, 
                lambda: self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_key)
            )
            return True
        except ClientError:
            return False

    async def _upload_to_s3_async(self, local_path: str, s3_key: str):
        """Async wrapper for S3 upload"""
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(
                None, 
                lambda: self.s3_client.upload_file(local_path, self.s3_bucket, s3_key)
            )
        except ClientError as e:
            logger.error(f"Failed to upload to S3: {str(e)}")
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
                "step": "tournament_scraping",
                "method": "aiohttp"
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
    
    logger.info(f"Starting tournament scraper for {args.month}")
    
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
        logger.error(f"Tournament scraping failed with error: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    exit(main())