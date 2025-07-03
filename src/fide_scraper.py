import os
import json
import argparse
import requests
import boto3
import logging
from datetime import datetime, date
from pathlib import Path
from typing import List
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
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
        self.max_workers = 5  # Reduced for debugging
        
    def is_valid_rating_period(self, year: int, month: int) -> bool:
        """
        Validate if the given year and month combination is a valid FIDE rating period.
        """
        # Check if the date is in the future
        current_date = date.today()
        request_date = date(year, month, 1)
        
        if request_date > current_date:
            logger.warning(f"Requested date {year}-{month:02d} is in the future. Current date: {current_date}")
            return False
            
        if year < 2009:
            return month % 3 == 1
        if year == 2009:
            return month < 7 and month % 3 == 1 or month >= 7 and month % 2 == 1
        if year < 2012:
            return month % 2 == 1
        if year == 2012:
            return (month < 7 and month % 2 == 1) or month >= 7
        return True

    def scrape_tournaments_for_month(self, month_str):
        """Scrape tournament data for all countries for a specific month"""
        try:
            year, month = map(int, month_str.split("-"))
            logger.info(f"Starting tournament scraping for {year}-{month:02d}")
            
            if not self.is_valid_rating_period(year, month):
                logger.error(f"Invalid rating period: {year}-{month:02d}, skipping")
                return False
            
            # Setup local temp directory
            os.makedirs(self.local_temp_dir, exist_ok=True)
            
            # Test with just a few countries first
            test_countries = countries[:5]  # Test with first 5 countries
            logger.info(f"Testing with {len(test_countries)} countries: {test_countries}")
            
            successful = 0
            failed = 0
            skipped = 0
            
            # Use ThreadPoolExecutor for concurrent scraping
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all tasks
                future_to_country = {
                    executor.submit(self._scrape_country_tournaments, country, year, month): country
                    for country in test_countries
                }
                
                # Process completed tasks
                for future in as_completed(future_to_country):
                    country = future_to_country[future]
                    try:
                        result = future.result()
                        if result is True:
                            successful += 1
                        elif result is None:
                            skipped += 1
                        else:
                            failed += 1
                            logger.error(f"Failed to scrape {country}: {result}")
                    except Exception as e:
                        failed += 1
                        logger.error(f"Exception scraping {country}: {str(e)}")
            
            logger.info(f"Tournament scraping completed: {successful} successful, {failed} failed, {skipped} skipped")
            
            # Upload completion marker
            self._upload_completion_marker(month_str, successful, failed, skipped)
            
            return True
                
        except Exception as e:
            logger.error(f"Error in tournament scraping: {str(e)}")
            raise

    def _scrape_country_tournaments(self, country: str, year: int, month: int):
        """Scrape tournament data for a specific country"""
        month_str = f"{month:02d}"
        s3_key = f"persistent/tournament_data/raw/{country}/{year}-{month_str}/tournaments.txt"
        
        logger.info(f"Processing {country} for {year}-{month_str}")
        
        # Check if data already exists in S3
        if self._check_s3_file_exists(s3_key):
            logger.info(f"Tournament data already exists for {country} {year}-{month_str}")
            return None  # Skipped
        
        try:
            # Make API request to FIDE
            api_url = "https://ratings.fide.com/a_tournaments.php"
            params = {
                "country": country,
                "period": f"{year}-{month_str}-01"
            }
            
            headers = {
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": f"https://ratings.fide.com/rated_tournaments.phtml?country={country}&period={year}-{month_str}-01",
                "User-Agent": "FIDE Tournament Scraper 1.0"
            }
            
            logger.info(f"Making request to FIDE for {country}: {api_url}?{requests.compat.urlencode(params)}")
            
            response = requests.get(api_url, params=params, headers=headers, timeout=30)
            
            logger.info(f"Response for {country}: Status {response.status_code}, Content-Type: {response.headers.get('content-type', 'unknown')}")
            logger.info(f"Response content preview for {country}: {response.text[:200]}...")
            
            response.raise_for_status()
            
            content = response.text
            if not content:
                logger.warning(f"Empty response for {country} {year}-{month_str}")
                return f"Empty response"
            
            # Process the response
            tournament_ids = self._extract_tournament_ids(content, country, year, month)
            
            if tournament_ids:
                # Save to local file
                local_file = self._save_tournaments_locally(tournament_ids, country, year, month)
                
                # Upload to S3
                self._upload_to_s3(local_file, s3_key)
                
                # Cleanup local file
                os.remove(local_file)
                
                logger.info(f"Saved {len(tournament_ids)} tournaments for {country} {year}-{month_str}")
            else:
                logger.info(f"No tournaments found for {country} {year}-{month_str}")
            
            # Small delay to be respectful to FIDE servers
            time.sleep(0.2)
            
            return True
                
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error for {country} {year}-{month_str}: {str(e)}")
            return str(e)
        except Exception as e:
            logger.error(f"Unexpected error for {country} {year}-{month_str}: {str(e)}")
            return str(e)

    def _extract_tournament_ids(self, content: str, country: str, year: int, month: int) -> List[str]:
        """Extract tournament IDs from FIDE API response"""
        try:
            logger.debug(f"Extracting tournament IDs for {country} from content: {content[:100]}...")
            
            # Clean up HTML entities
            content = content.replace("</a>", "")
            content = content.replace("&lt;", "<")
            content = content.replace("&gt;", ">")
            
            # Parse JSON
            data = json.loads(content)
            tournament_ids = []
            
            logger.debug(f"Parsed JSON structure for {country}: {type(data)} with keys: {list(data.keys()) if isinstance(data, dict) else 'not a dict'}")
            
            # Extract tournament IDs from the data
            if "data" in data and isinstance(data["data"], list):
                logger.info(f"Found {len(data['data'])} tournament entries for {country}")
                for i, tournament in enumerate(data["data"]):
                    if isinstance(tournament, list) and len(tournament) > 0:
                        tournament_ids.append(str(tournament[0]))
                        if i < 3:  # Log first few for debugging
                            logger.debug(f"Tournament {i} for {country}: {tournament[0]}")
            else:
                logger.warning(f"No 'data' field or invalid structure for {country}. Available keys: {list(data.keys()) if isinstance(data, dict) else 'none'}")
            
            return tournament_ids
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error for {country} {year}-{month}: {str(e)}")
            logger.error(f"Content that failed to parse: {content[:500]}...")
            return []
        except Exception as e:
            logger.error(f"Error extracting tournament IDs for {country} {year}-{month}: {str(e)}")
            return []

    def _save_tournaments_locally(self, tournament_ids: List[str], country: str, year: int, month: int) -> str:
        """Save tournament IDs to local file"""
        month_str = f"{month:02d}"
        local_dir = os.path.join(self.local_temp_dir, "tournaments", country, f"{year}-{month_str}")
        os.makedirs(local_dir, exist_ok=True)
        
        local_file = os.path.join(local_dir, "tournaments.txt")
        
        with open(local_file, "w") as f:
            f.write("\n".join(tournament_ids))
        
        return local_file

    def _check_s3_file_exists(self, s3_key: str) -> bool:
        """Check if file exists in S3"""
        try:
            self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_key)
            return True
        except ClientError:
            return False

    def _upload_to_s3(self, local_path: str, s3_key: str):
        """Upload file to S3"""
        try:
            self.s3_client.upload_file(local_path, self.s3_bucket, s3_key)
            logger.debug(f"Uploaded {local_path} to S3 as {s3_key}")
        except ClientError as e:
            logger.error(f"Failed to upload {local_path} to S3: {str(e)}")
            raise

    def _upload_completion_marker(self, month_str: str, successful: int, failed: int, skipped: int):
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
            with open(local_file, "w") as f:
                f.write(json.dumps(completion_data, indent=2))
            
            s3_key = f"results/{month_str}/tournament_scraping_completion.json"
            self._upload_to_s3(local_file, s3_key)
            
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
        success = scraper.scrape_tournaments_for_month(args.month)
        
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