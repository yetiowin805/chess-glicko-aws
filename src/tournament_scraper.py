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
from typing import List, Tuple, Optional
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
import re
from countries import countries

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_AWS_REGION = "us-east-2"
BASE_URL = "https://ratings.fide.com"
TOURNAMENT_REPORT_PATH = "report.phtml?event="

class FIDETournamentScraper:
    def __init__(self, s3_bucket, aws_region=DEFAULT_AWS_REGION):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3', region_name=aws_region)
        self.local_temp_dir = "/tmp/chess_data"
        
        # Semaphores for different levels of concurrency
        self.country_semaphore = asyncio.Semaphore(20)  # Max countries processed simultaneously
        self.tournament_semaphore = asyncio.Semaphore(50)  # Max tournaments processed simultaneously
        
        logger.info(f"Initialized tournament scraper - S3 bucket: {s3_bucket}, Region: {aws_region}")
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

    async def process_tournaments_for_month(self, month_str):
        """Process tournament data for all countries for a specific month"""
        try:
            year, month = map(int, month_str.split("-"))
            logger.info(f"Starting tournament processing for {year}-{month:02d}")
            
            if not self.is_valid_rating_period(year, month):
                logger.warning(f"Invalid rating period: {year}-{month:02d}, skipping")
                return True
            
            # Setup local temp directory
            os.makedirs(self.local_temp_dir, exist_ok=True)
            
            # Configure async session with higher limits for combined processing
            connector = aiohttp.TCPConnector(limit=200, limit_per_host=30)
            timeout = aiohttp.ClientTimeout(total=60)
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                # Create tasks for all countries - each country processes completely independently
                tasks = [
                    self._process_country_tournaments(session, country, year, month)
                    for country in countries
                ]
                
                logger.info(f"Processing tournaments for {len(tasks)} countries in parallel")
                
                # Execute all country tasks concurrently
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results and log summary
                successful = []
                failed = []
                skipped = []
                
                for i, result in enumerate(results):
                    country = countries[i]
                    if isinstance(result, tuple) and result[0] == "success":
                        successful.append((country, result[1]))  # (country, tournament_count)
                    elif result == "skipped":
                        skipped.append(country)
                    else:
                        failed.append((country, str(result) if isinstance(result, Exception) else result))
                
                total_tournaments = sum(count for _, count in successful)
                logger.info(f"Results - Countries: {len(successful)} successful, {len(skipped)} skipped, {len(failed)} failed")
                logger.info(f"Total tournaments processed: {total_tournaments}")
                
                # Log failures for debugging
                if failed:
                    logger.error(f"Failed countries: {[f[0] for f in failed[:10]]}")
                    for country, error in failed[:3]:
                        logger.error(f"  {country}: {error}")
                
                # Upload completion marker
                await self._upload_completion_marker(month_str, len(successful), len(failed), len(skipped), total_tournaments)
                
                return True
                
        except Exception as e:
            logger.error(f"Error in tournament processing: {str(e)}", exc_info=True)
            raise

    async def _process_country_tournaments(self, session: aiohttp.ClientSession, country: str, year: int, month: int):
        """Process all tournaments for a specific country - fetch tournament list then process each tournament"""
        month_str = f"{month:02d}"
        
        async with self.country_semaphore:
            try:
                # Step 1: Get tournament IDs for this country
                tournament_ids = await self._fetch_tournament_ids(session, country, year, month)
                if not tournament_ids:
                    return "skipped"
                
                logger.info(f"Found {len(tournament_ids)} tournaments for {country}, processing in parallel")
                
                # Step 2: Process all tournaments for this country in parallel
                tasks = [
                    self._process_single_tournament(session, tournament_id, country, year, month)
                    for tournament_id in tournament_ids
                ]
                
                # Execute all tournament tasks for this country concurrently
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Count successful downloads
                successful_count = sum(1 for result in results if result is True)
                failed_count = len(results) - successful_count
                
                if failed_count > 0:
                    logger.warning(f"{country}: {successful_count}/{len(results)} tournaments processed successfully")
                else:
                    logger.info(f"{country}: All {successful_count} tournaments processed successfully")
                
                return ("success", successful_count)
                
            except Exception as e:
                logger.error(f"Error processing country {country}: {str(e)}")
                return e

    async def _fetch_tournament_ids(self, session: aiohttp.ClientSession, country: str, year: int, month: int) -> List[str]:
        """Fetch tournament IDs for a country from FIDE API"""
        month_str = f"{month:02d}"
        
        if not self.is_valid_rating_period(year, month):
            return []
        
        api_url = "https://ratings.fide.com/a_tournaments.php"
        params = {
            "country": country,
            "period": f"{year}-{month_str}-01"
        }
        
        try:
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
                    return []
                
                content = await api_response.text()
                
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.warning(f"Failed to fetch tournament list for {country}: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error fetching tournaments for {country}: {str(e)}")
            return []

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
                return []

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
            
            return tournament_ids
            
        except Exception as e:
            logger.error(f"Error processing tournament list for {country}: {str(e)}")
            return []

    async def _process_single_tournament(self, session: aiohttp.ClientSession, tournament_id: str, country: str, year: int, month: int) -> bool:
        """Process a single tournament and save player data"""
        month_str = f"{month:02d}"
        url = f"{BASE_URL}/{TOURNAMENT_REPORT_PATH}{tournament_id}"
        
        # Check if already processed (check all time control directories)
        s3_key_pattern = f"persistent/tournament_data/processed/{{}}/{year}-{month_str}/{tournament_id}.txt"
        
        for tc in ["standard", "rapid", "blitz"]:
            s3_key = s3_key_pattern.format(tc)
            if await self._check_s3_file_exists_async(s3_key):
                return True  # Already processed
        
        max_retries = 3
        base_delay = 1
        
        async with self.tournament_semaphore:
            for attempt in range(max_retries):
                try:
                    async with session.get(url, timeout=30) as response:
                        response.raise_for_status()
                        text = await response.text()
                        soup = BeautifulSoup(text, "html.parser")

                        # Extract player data and time control
                        player_data, time_control = await self._extract_player_data(soup)

                        if not player_data:
                            # Not necessarily an error - tournament might have no players
                            return True

                        # Map time_control value to directory name
                        time_control_dirs = {"0": "standard", "1": "rapid", "2": "blitz"}
                        tc_dir = time_control_dirs.get(time_control, "standard")

                        # Save to local file first
                        local_file = await self._save_tournament_locally(player_data, tournament_id, tc_dir, year, month)
                        
                        # Upload to S3
                        s3_key = f"persistent/tournament_data/processed/{tc_dir}/{year}-{month_str}/{tournament_id}.txt"
                        await self._upload_to_s3_async(local_file, s3_key)
                        
                        # Cleanup local file
                        os.remove(local_file)
                        
                        return True

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    delay = base_delay * (2 ** attempt)
                    if attempt < max_retries - 1:
                        await asyncio.sleep(delay)
                    else:
                        logger.warning(f"Failed to process tournament {tournament_id} after {max_retries} attempts: {e}")
                        return False
                except Exception as e:
                    logger.error(f"Unexpected error processing tournament {tournament_id}: {str(e)}")
                    return False

        return False

    async def _extract_player_data(self, soup: BeautifulSoup) -> Tuple[List[Tuple[str, str]], Optional[str]]:
        """Extract player IDs and names from tournament HTML"""
        player_data = []
        time_control = None
        table = soup.find("table", {"class": "table2"})

        if table is None:
            return player_data, time_control

        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if cells and len(cells) > 1:
                # Extract player ID
                player_id = cells[0].text.strip()
                if not player_id.isdigit():
                    continue

                # Extract player name
                name_cell = cells[1]
                player_name = name_cell.get_text(strip=True)

                player_data.append((player_id, player_name))

                # Extract time control from the first valid player link
                if time_control is None:
                    href_tag = cells[1].find("a")
                    if href_tag and href_tag.get("href"):
                        href = href_tag["href"]
                        match = re.search(r"rating=(\d+)", href)
                        if match:
                            time_control = match.group(1)

        return player_data, time_control

    async def _save_tournament_locally(self, player_data: List[Tuple[str, str]], tournament_id: str, tc_dir: str, year: int, month: int) -> str:
        """Save tournament player data to local file"""
        month_str = f"{month:02d}"
        local_dir = os.path.join(self.local_temp_dir, "processed_tournaments", tc_dir, f"{year}-{month_str}")
        os.makedirs(local_dir, exist_ok=True)
        
        local_file = os.path.join(local_dir, f"{tournament_id}.txt")
        
        async with aiofiles.open(local_file, "w", encoding="utf-8") as f:
            for player_id, player_name in player_data:
                player_record = {"id": player_id, "name": player_name}
                await f.write(json.dumps(player_record) + "\n")
        
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

    async def _upload_completion_marker(self, month_str: str, successful: int, failed: int, skipped: int, total_tournaments: int):
        """Upload completion marker with statistics"""
        try:
            completion_data = {
                "month": month_str,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "statistics": {
                    "countries_processed": len(countries),
                    "countries_successful": successful,
                    "countries_failed": failed,
                    "countries_skipped": skipped,
                    "total_tournaments_processed": total_tournaments
                },
                "step": "combined_tournament_processing",
                "method": "aiohttp_parallel"
            }
            
            local_file = os.path.join(self.local_temp_dir, f"tournament_processing_completion_{month_str}.json")
            async with aiofiles.open(local_file, "w") as f:
                await f.write(json.dumps(completion_data, indent=2))
            
            s3_key = f"results/{month_str}/tournament_processing_completion.json"
            await self._upload_to_s3_async(local_file, s3_key)
            
            os.remove(local_file)
            logger.info(f"Uploaded completion marker for {month_str}")
            
        except Exception as e:
            logger.warning(f"Failed to upload completion marker: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Scrape and process FIDE tournament data in one step.")
    parser.add_argument(
        "--month",
        type=str,
        help="Month for processing in YYYY-MM format",
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
    
    logger.info(f"Starting combined tournament scraper for {args.month}")
    
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
    
    # Run processing
    try:
        success = asyncio.run(scraper.process_tournaments_for_month(args.month))
        
        if success:
            logger.info(f"Combined tournament processing completed successfully for {args.month}")
            return 0
        else:
            logger.error("Combined tournament processing failed")
            return 1
            
    except Exception as e:
        logger.error(f"Combined tournament processing failed with error: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    exit(main())