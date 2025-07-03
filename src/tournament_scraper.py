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

class FIDETournamentProcessor:
    def __init__(self, s3_bucket, aws_region=DEFAULT_AWS_REGION):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3', region_name=aws_region)
        self.local_temp_dir = "/tmp/chess_data"
        self.max_concurrent_requests = 10
        self.semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        
        logger.info(f"Initialized tournament processor - S3 bucket: {s3_bucket}, Region: {aws_region}")
        logger.info(f"Max concurrent requests: {self.max_concurrent_requests}")

    async def process_tournaments_for_month(self, month_str):
        """Process tournament data for all countries for a specific month"""
        try:
            year, month = map(int, month_str.split("-"))
            logger.info(f"Starting tournament processing for {year}-{month:02d}")
            
            # Setup local temp directory
            os.makedirs(self.local_temp_dir, exist_ok=True)
            
            # Configure async session
            connector = aiohttp.TCPConnector(limit=100)
            timeout = aiohttp.ClientTimeout(total=60)
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                # Create tasks for all countries
                tasks = [
                    self._process_country_tournaments(session, country, year, month)
                    for country in countries
                ]
                
                logger.info(f"Processing tournaments for {len(tasks)} countries")
                
                # Execute all tasks concurrently
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
        """Process tournament data for a specific country"""
        month_str = f"{month:02d}"
        
        # Download tournament IDs from S3
        tournament_ids = await self._download_tournament_ids(country, year, month)
        if not tournament_ids:
            return "skipped"
        
        logger.info(f"Processing {len(tournament_ids)} tournaments for {country}")
        
        # Process each tournament
        tasks = [
            self._process_single_tournament(session, tournament_id, country, year, month)
            for tournament_id in tournament_ids
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Count successful downloads
        successful_count = sum(1 for result in results if result is True)
        failed_count = len(results) - successful_count
        
        if failed_count > 0:
            logger.warning(f"{country}: {successful_count}/{len(results)} tournaments processed successfully")
        else:
            logger.info(f"{country}: All {successful_count} tournaments processed successfully")
        
        return ("success", successful_count)

    async def _download_tournament_ids(self, country: str, year: int, month: int) -> List[str]:
        """Download tournament IDs for a country from S3"""
        month_str = f"{month:02d}"
        s3_key = f"persistent/tournament_data/raw/{country}/{year}-{month_str}/tournaments.txt"
        
        try:
            # Download from S3
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            )
            
            # Read content
            content = response['Body'].read().decode('utf-8')
            tournament_ids = [line.strip() for line in content.splitlines() if line.strip()]
            
            return tournament_ids
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.info(f"No tournament data found for {country} {year}-{month_str}")
                return []
            else:
                logger.error(f"Error downloading tournament IDs for {country}: {str(e)}")
                return []
        except Exception as e:
            logger.error(f"Unexpected error downloading tournament IDs for {country}: {str(e)}")
            return []

    async def _process_single_tournament(self, session: aiohttp.ClientSession, tournament_id: str, country: str, year: int, month: int) -> bool:
        """Process a single tournament and save player data"""
        month_str = f"{month:02d}"
        url = f"{BASE_URL}/{TOURNAMENT_REPORT_PATH}{tournament_id}"
        
        # Check if already processed
        s3_key_pattern = f"persistent/tournament_data/processed/{{}}/{year}-{month_str}/{tournament_id}.txt"
        
        # Check all time control directories
        for tc in ["standard", "rapid", "blitz"]:
            s3_key = s3_key_pattern.format(tc)
            if await self._check_s3_file_exists_async(s3_key):
                return True  # Already processed
        
        max_retries = 3
        base_delay = 1
        
        async with self.semaphore:
            for attempt in range(max_retries):
                try:
                    async with session.get(url, timeout=30) as response:
                        response.raise_for_status()
                        text = await response.text()
                        soup = BeautifulSoup(text, "html.parser")

                        # Extract player data and time control
                        player_data, time_control = await self._extract_player_data(soup)

                        if not player_data:
                            logger.warning(f"No player data found for tournament {tournament_id}")
                            return False

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
                "step": "tournament_processing",
                "method": "aiohttp"
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
    parser = argparse.ArgumentParser(description="Process FIDE tournament data from tournament IDs.")
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
    
    logger.info(f"Starting tournament processor for {args.month}")
    
    # Validate month format
    try:
        datetime.strptime(args.month, "%Y-%m")
    except ValueError:
        logger.error("Month must be in YYYY-MM format")
        return 1
    
    # Initialize processor
    processor = FIDETournamentProcessor(
        s3_bucket=args.s3_bucket,
        aws_region=args.aws_region
    )
    
    # Run processing
    try:
        success = asyncio.run(processor.process_tournaments_for_month(args.month))
        
        if success:
            logger.info(f"Tournament processing completed successfully for {args.month}")
            return 0
        else:
            logger.error("Tournament processing failed")
            return 1
            
    except Exception as e:
        logger.error(f"Tournament processing failed with error: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    exit(main())