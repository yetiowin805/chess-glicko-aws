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

# Configure detailed logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_AWS_REGION = "us-east-2"

class FIDETournamentScraper:
    def __init__(self, s3_bucket, aws_region=DEFAULT_AWS_REGION):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3', region_name=aws_region)
        self.local_temp_dir = "/tmp/chess_data"
        
        # Async configuration - match your local script
        self.max_concurrent_requests = 5  # Reduced for better debugging
        self.semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        
        logger.info(f"Initialized scraper - S3 bucket: {s3_bucket}, Region: {aws_region}")
        logger.info(f"Max concurrent requests: {self.max_concurrent_requests}")
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
        
        logger.debug(f"Rating period validation for {year}-{month:02d}: {valid}")
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
            logger.debug(f"Created temp directory: {self.local_temp_dir}")
            
            # Configure async session exactly like your local script
            connector = aiohttp.TCPConnector(limit=100)
            timeout = aiohttp.ClientTimeout(total=60)
            
            logger.info(f"Starting scraping with {len(countries)} countries")
            logger.debug(f"First 10 countries: {countries[:10]}")
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                # Create tasks for all countries
                tasks = [
                    self._scrape_country_tournaments(session, country, year, month)
                    for country in countries
                ]
                
                logger.info(f"Created {len(tasks)} tasks")
                
                # Execute all tasks concurrently
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                logger.info(f"Completed all tasks, processing {len(results)} results")
                
                # Process results and log summary with detailed breakdown
                successful = []
                failed = []
                skipped = []
                errors = []
                
                for i, result in enumerate(results):
                    country = countries[i]
                    if result is True:
                        successful.append(country)
                    elif result is None:
                        skipped.append(country)
                    elif isinstance(result, Exception):
                        failed.append((country, str(result)))
                    else:
                        errors.append((country, result))
                
                logger.info(f"=== DETAILED RESULTS BREAKDOWN ===")
                logger.info(f"Successful ({len(successful)}): {successful[:10]}{'...' if len(successful) > 10 else ''}")
                logger.info(f"Skipped ({len(skipped)}): {skipped[:10]}{'...' if len(skipped) > 10 else ''}")
                logger.info(f"Failed ({len(failed)}): {failed[:5]}{'...' if len(failed) > 5 else ''}")
                logger.info(f"Errors ({len(errors)}): {errors[:5]}{'...' if len(errors) > 5 else ''}")
                
                # Log detailed failure information
                if failed:
                    logger.error("=== FAILURE DETAILS ===")
                    for country, error in failed[:10]:  # Show first 10 failures
                        logger.error(f"  {country}: {error}")
                
                # Upload completion marker
                await self._upload_completion_marker(month_str, len(successful), len(failed), len(skipped))
                
                return True
                
        except Exception as e:
            logger.error(f"Error in tournament scraping: {str(e)}", exc_info=True)
            raise

    async def _scrape_country_tournaments(self, session: aiohttp.ClientSession, country: str, year: int, month: int):
        """Scrape tournament data for a specific country - matches your local script"""
        month_str = f"{month:02d}"
        logger.debug(f"Processing {country} for {year}-{month_str}")
        
        if not self.is_valid_rating_period(year, month):
            logger.debug(f"Invalid rating period for {country}, skipping")
            return None
            
        s3_key = f"persistent/tournament_data/raw/{country}/{year}-{month_str}/tournaments.txt"
        
        # Check if data already exists in S3
        if await self._check_s3_file_exists_async(s3_key):
            logger.debug(f"Tournament data already exists for {country} {year}-{month_str}")
            return None  # Skipped
        
        api_url = "https://ratings.fide.com/a_tournaments.php"
        params = {
            "country": country,
            "period": f"{year}-{month_str}-01"
        }
        
        logger.debug(f"Making request for {country}: {api_url} with params {params}")
        
        try:
            async with self.semaphore:
                # Use exactly the same headers as your local script
                headers = {
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": f"https://ratings.fide.com/rated_tournaments.phtml?country={country}&period={year}-{month_str}-01"
                }
                
                logger.debug(f"Request headers for {country}: {headers}")
                
                async with session.get(api_url, params=params, headers=headers, timeout=30) as api_response:
                    logger.debug(f"Response status for {country}: {api_response.status}")
                    logger.debug(f"Response headers for {country}: {dict(api_response.headers)}")
                    
                    api_response.raise_for_status()
                    
                    # Read the raw bytes first (like your local script)
                    raw_content = await api_response.read()
                    if not raw_content:
                        logger.error(f"Empty response received for {country} {year}-{month_str}")
                        return Exception("Empty response")
                    
                    logger.debug(f"Raw content length for {country}: {len(raw_content)} bytes")
                    
                    # Decode the content, handling gzip compression
                    content = await api_response.text()
                    logger.debug(f"Decoded content length for {country}: {len(content)} characters")
                    logger.debug(f"Raw content preview for {country}: {content[:300]}...")
                    
        except aiohttp.ClientError as e:
            logger.error(f"HTTP error occurred for {country} {year}-{month_str}: {str(e)}")
            return e
        except asyncio.TimeoutError:
            logger.error(f"Request timed out for {country} {year}-{month_str}")
            return Exception("Timeout")
        except Exception as e:
            logger.error(f"Unexpected error for {country} {year}-{month_str}: {str(e)}", exc_info=True)
            return e

        # Process exactly like your local script
        try:
            logger.debug(f"Processing response content for {country}")
            
            # Log original content before processing
            logger.debug(f"Original content for {country}: {content[:500]}...")
            
            content = content.replace("</a>", "")
            content = content.replace("&lt;", "<")
            content = content.replace("&gt;", ">")
            
            logger.debug(f"Cleaned content for {country}: {content[:500]}...")

            try:
                data = json.loads(content)
                logger.debug(f"JSON parsed successfully for {country}")
                logger.debug(f"JSON structure for {country}: {type(data)}")
                
                if isinstance(data, dict):
                    logger.debug(f"JSON keys for {country}: {list(data.keys())}")
                    for key, value in data.items():
                        if isinstance(value, list):
                            logger.debug(f"  {key}: list with {len(value)} items")
                        else:
                            logger.debug(f"  {key}: {type(value)} = {value}")
                else:
                    logger.debug(f"JSON data for {country} is not a dict: {data}")
                    
            except json.JSONDecodeError as je:
                logger.error(f"JSON decode failed for {country}: {str(je)}")
                logger.error(f"Content that failed: {content}")
                return je

            tournament_ids = []

            # Extract tournament IDs from the data (your local script format)
            if "data" in data and isinstance(data["data"], list):
                logger.info(f"Found 'data' field for {country} with {len(data['data'])} tournaments")
                for i, tournament in enumerate(data["data"]):
                    if isinstance(tournament, list) and len(tournament) > 0:
                        tournament_ids.append(str(tournament[0]))
                        if i < 3:  # Log first few tournaments
                            logger.debug(f"  Tournament {i} for {country}: {tournament[0]} (full: {tournament})")
                            
                logger.info(f"Extracted {len(tournament_ids)} tournament IDs for {country}")
            elif "aaData" in data and isinstance(data["aaData"], list):
                logger.info(f"Found 'aaData' field for {country} with {len(data['aaData'])} tournaments")
                for i, tournament in enumerate(data["aaData"]):
                    if isinstance(tournament, list) and len(tournament) > 0:
                        tournament_ids.append(str(tournament[0]))
                        if i < 3:  # Log first few tournaments
                            logger.debug(f"  Tournament {i} for {country}: {tournament[0]} (full: {tournament})")
                            
                logger.info(f"Extracted {len(tournament_ids)} tournament IDs from aaData for {country}")
            else:
                logger.warning(f"No 'data' or 'aaData' field found for {country}")
                logger.warning(f"Available fields: {list(data.keys()) if isinstance(data, dict) else 'not a dict'}")
                # Still return True for success, just no tournaments
                return True

            if tournament_ids:
                logger.info(f"Saving {len(tournament_ids)} tournaments for {country}")
                
                # Save to local file
                local_file = await self._save_tournaments_locally(tournament_ids, country, year, month)
                logger.debug(f"Saved to local file: {local_file}")
                
                # Upload to S3
                await self._upload_to_s3_async(local_file, s3_key)
                logger.debug(f"Uploaded to S3: {s3_key}")
                
                # Cleanup local file
                os.remove(local_file)
                logger.debug(f"Cleaned up local file: {local_file}")
                
                logger.info(f"Successfully saved {len(tournament_ids)} tournaments for {country} {year}-{month_str}")
            else:
                logger.info(f"No tournaments found for {country} {year}-{month_str}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing response for {country} {year}-{month}: {str(e)}", exc_info=True)
            return e

    async def _save_tournaments_locally(self, tournament_ids: List[str], country: str, year: int, month: int) -> str:
        """Save tournament IDs to local file"""
        month_str = f"{month:02d}"
        local_dir = os.path.join(self.local_temp_dir, "tournaments", country, f"{year}-{month_str}")
        os.makedirs(local_dir, exist_ok=True)
        
        local_file = os.path.join(local_dir, "tournaments.txt")
        
        logger.debug(f"Saving {len(tournament_ids)} IDs to {local_file}")
        
        async with aiofiles.open(local_file, "w") as f:
            await f.write("\n".join(tournament_ids))
        
        return local_file

    async def _check_s3_file_exists_async(self, s3_key: str) -> bool:
        """Async wrapper for checking S3 file existence"""
        loop = asyncio.get_event_loop()
        try:
            # Fix: Use a lambda to pass keyword arguments correctly
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
                "step": "tournament_scraping",
                "method": "aiohttp_debug"
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
    
    logger.info(f"=== STARTING TOURNAMENT SCRAPER ===")
    logger.info(f"Month: {args.month}")
    logger.info(f"S3 Bucket: {args.s3_bucket}")
    logger.info(f"AWS Region: {args.aws_region}")
    
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