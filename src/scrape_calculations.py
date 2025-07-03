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
from typing import List, Dict, Optional
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_AWS_REGION = "us-east-2"
BASE_URL = "https://ratings.fide.com/a_indv_calculations.php"
TIME_CONTROLS = {"standard": "0", "rapid": "1", "blitz": "2"}

class PlayerCalculationScraper:
    def __init__(self, s3_bucket, aws_region=DEFAULT_AWS_REGION):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3', region_name=aws_region)
        self.local_temp_dir = "/tmp/chess_data"
        
        # High concurrency for this intensive step, but respect server limits
        self.max_concurrent_requests = 30
        self.semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        
        logger.info(f"Initialized calculation scraper - S3 bucket: {s3_bucket}, Region: {aws_region}")
        logger.info(f"Max concurrent requests: {self.max_concurrent_requests}")

    async def scrape_calculations_for_month(self, month_str):
        """Scrape calculation data for all active players for a specific month"""
        try:
            year, month = map(int, month_str.split("-"))
            logger.info(f"Starting calculation scraping for {year}-{month:02d}")
            
            # Setup local temp directory
            os.makedirs(self.local_temp_dir, exist_ok=True)
            
            # Determine which time controls to process based on year
            time_controls_to_process = ["standard"]
            if year >= 2012:
                time_controls_to_process.extend(["rapid", "blitz"])
            
            logger.info(f"Processing time controls: {time_controls_to_process}")
            
            # Configure async session with higher limits for this intensive step
            connector = aiohttp.TCPConnector(
                limit=200,          # Total connection pool size
                limit_per_host=50,  # Connections per host (FIDE server)
                ttl_dns_cache=300,  # DNS cache TTL
                use_dns_cache=True
            )
            timeout = aiohttp.ClientTimeout(total=60)
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                # Process each time control in parallel
                tasks = [
                    self._process_time_control(session, tc, year, month)
                    for tc in time_controls_to_process
                ]
                
                logger.info(f"Processing {len(time_controls_to_process)} time controls in parallel")
                
                # Execute all time control tasks concurrently
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Process results and log summary
                successful = []
                failed = []
                
                for i, result in enumerate(results):
                    tc = time_controls_to_process[i]
                    if isinstance(result, tuple) and result[0] == "success":
                        successful.append((tc, result[1], result[2]))  # (tc, processed, successful)
                    elif result == "skipped":
                        logger.info(f"No active players found for {tc}")
                    else:
                        failed.append((tc, str(result) if isinstance(result, Exception) else result))
                
                total_processed = sum(processed for _, processed, _ in successful)
                total_successful = sum(successful_count for _, _, successful_count in successful)
                
                logger.info(f"Results - Time controls: {len(successful)} successful, {len(failed)} failed")
                logger.info(f"Total calculations: {total_processed} processed, {total_successful} successful")
                
                # Log failures for debugging
                if failed:
                    logger.error(f"Failed time controls: {[f[0] for f in failed]}")
                    for tc, error in failed:
                        logger.error(f"  {tc}: {error}")
                
                # Upload completion marker
                await self._upload_completion_marker(month_str, len(successful), len(failed), successful)
                
                return True
                
        except Exception as e:
            logger.error(f"Error in calculation scraping: {str(e)}", exc_info=True)
            raise

    async def _process_time_control(self, session: aiohttp.ClientSession, time_control: str, year: int, month: int):
        """Process all players for a specific time control"""
        month_str = f"{month:02d}"
        
        try:
            # Download active player list from S3
            player_ids = await self._download_active_players(time_control, year, month)
            
            if not player_ids:
                logger.info(f"No active players found for {time_control} {year}-{month_str}")
                return "skipped"
            
            logger.info(f"Processing {len(player_ids)} players for {time_control}")
            
            # Process players in optimized batches
            batch_size = 100  # Process 100 players at a time
            total_batches = (len(player_ids) + batch_size - 1) // batch_size
            total_processed = 0
            total_successful = 0
            
            for i in range(0, len(player_ids), batch_size):
                batch = player_ids[i:i + batch_size]
                batch_num = i // batch_size + 1
                
                logger.info(f"{time_control}: Processing batch {batch_num}/{total_batches} ({len(batch)} players)")
                
                # Create tasks for this batch
                tasks = [
                    self._process_single_player(session, player_id, time_control, year, month)
                    for player_id in batch
                ]
                
                # Execute batch concurrently
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Count results
                batch_successful = sum(1 for result in results if result is True)
                batch_processed = len(results)
                
                total_processed += batch_processed
                total_successful += batch_successful
                
                logger.info(f"{time_control}: Batch {batch_num} completed - "
                          f"{batch_successful}/{batch_processed} successful, "
                          f"Total: {total_successful}/{total_processed}")
                
                # Brief pause between batches to be respectful to the server
                if batch_num < total_batches:
                    await asyncio.sleep(0.5)
            
            logger.info(f"{time_control}: Completed - {total_successful}/{total_processed} players processed successfully")
            
            return ("success", total_processed, total_successful)
            
        except Exception as e:
            logger.error(f"Error processing time control {time_control}: {str(e)}")
            return e

    async def _download_active_players(self, time_control: str, year: int, month: int) -> List[str]:
        """Download active player list for a time control from S3"""
        month_str = f"{month:02d}"
        s3_key = f"persistent/active_players/{year}-{month_str}_{time_control}.txt"
        
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            )
            
            content = response['Body'].read().decode('utf-8')
            player_ids = [line.strip() for line in content.splitlines() if line.strip()]
            
            return player_ids
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.info(f"No active players file found for {time_control} {year}-{month_str}")
                return []
            else:
                logger.error(f"Error downloading active players for {time_control}: {str(e)}")
                return []
        except Exception as e:
            logger.error(f"Unexpected error downloading active players for {time_control}: {str(e)}")
            return []

    async def _process_single_player(self, session: aiohttp.ClientSession, player_id: str, time_control: str, year: int, month: int) -> bool:
        """Process calculation data for a single player"""
        month_str = f"{month:02d}"
        
        # Check if already processed
        s3_key = f"persistent/calculations/{year}-{month_str}/{time_control}/{player_id}.json"
        if await self._check_s3_file_exists_async(s3_key):
            return True  # Already processed
        
        tc_code = TIME_CONTROLS[time_control]
        period_formatted = f"{year}-{month_str}-01"
        url = f"{BASE_URL}?id_number={player_id}&rating_period={period_formatted}&t={tc_code}"
        
        max_retries = 3
        base_delay = 1
        
        async with self.semaphore:
            for attempt in range(max_retries):
                try:
                    async with session.get(url, timeout=30) as response:
                        response.raise_for_status()
                        html_content = await response.text()

                        # Check if there's actual calculation data
                        if ("No calculations available" in html_content or 
                            "No games" in html_content or 
                            len(html_content.strip()) < 100):
                            # No calculation data - this is normal for many players
                            return True

                        # Extract structured data from the HTML
                        calculation_data = self._extract_calculation_data(html_content)

                        if not calculation_data.get("tournaments"):
                            # No tournaments found in the calculation
                            return True

                        # Save to local file first
                        local_file = await self._save_calculation_locally(calculation_data, player_id, time_control, year, month)
                        
                        # Upload to S3
                        await self._upload_to_s3_async(local_file, s3_key)
                        
                        # Cleanup local file
                        os.remove(local_file)
                        
                        return True

                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    delay = base_delay * (2 ** attempt)
                    if attempt < max_retries - 1:
                        await asyncio.sleep(delay)
                    else:
                        logger.warning(f"Failed to process player {player_id} after {max_retries} attempts: {e}")
                        return False
                except Exception as e:
                    logger.error(f"Unexpected error processing player {player_id}: {str(e)}")
                    return False

        return False

    def _extract_calculation_data(self, html_content: str) -> Dict:
        """Extract player calculation data from HTML content"""
        soup = BeautifulSoup(html_content, "html.parser")
        result_data = {
            "tournaments": [],
        }

        # First find all tournament headers
        tournament_headers = soup.find_all("div", class_="rtng_line01")
        tournament_ids = []
        tournament_names = []

        for header in tournament_headers:
            # Look for tournament links and extract IDs
            tournament_link = header.find("a")
            if tournament_link:
                href = tournament_link.get("href", "")
                tournament_id_match = re.search(r"event=(\d+)", href)
                if tournament_id_match:
                    tournament_id = tournament_id_match.group(1)
                    tournament_name = tournament_link.text.strip()
                    tournament_ids.append(tournament_id)
                    tournament_names.append(tournament_name)

        # Find all calculation tables
        calc_tables = soup.find_all("table", class_="calc_table")

        # Process each table and associate with tournament ID
        for i, calc_table in enumerate(calc_tables):
            if i < len(tournament_ids):  # Make sure we have a tournament ID for this table
                tournament_id = tournament_ids[i]

                # Create tournament entry
                current_tournament = {"tournament_id": tournament_id, "games": []}

                # Check if player is unrated in this tournament
                rp_headers = calc_table.find_all("td", string=lambda s: s and "Rp" in s)
                current_tournament["player_is_unrated"] = len(rp_headers) > 0

                # Find all rows with game data
                game_rows = calc_table.find_all("tr", attrs={"bgcolor": "#efefef"})

                for row in game_rows:
                    try:
                        cells = row.find_all("td", class_="list4")
                        if len(cells) < 6:  # Need at least name, rating, federation, result
                            continue

                        game = {}

                        # Extract opponent name
                        opponent_cell = cells[0]
                        game["opponent_name"] = opponent_cell.text.strip()

                        # Extract rating
                        if len(cells) > 3:
                            rating_text = cells[3].text.strip()
                            rating = re.search(r"-?\d+", rating_text)
                            if rating:
                                game["opponent_rating"] = rating.group()

                        # Extract federation
                        if len(cells) > 4:
                            game["federation"] = cells[4].text.strip()

                        # Extract game result
                        if len(cells) > 5:
                            game["result"] = cells[5].text.strip()

                        # Add tournament_id to each game
                        game["tournament_id"] = tournament_id

                        # Add to current tournament's games
                        current_tournament["games"].append(game)

                    except Exception as e:
                        logger.warning(f"Error extracting game data: {e}")

                # Add tournament to results if it has games
                if current_tournament["games"]:
                    result_data["tournaments"].append(current_tournament)

        return result_data

    async def _save_calculation_locally(self, calculation_data: Dict, player_id: str, time_control: str, year: int, month: int) -> str:
        """Save calculation data to local file"""
        month_str = f"{month:02d}"
        local_dir = os.path.join(self.local_temp_dir, "calculations", f"{year}-{month_str}", time_control)
        os.makedirs(local_dir, exist_ok=True)
        
        local_file = os.path.join(local_dir, f"{player_id}.json")
        
        async with aiofiles.open(local_file, "w", encoding="utf-8") as f:
            await f.write(json.dumps(calculation_data, indent=2))
        
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

    async def _upload_completion_marker(self, month_str: str, successful: int, failed: int, success_details: List):
        """Upload completion marker with statistics"""
        try:
            completion_data = {
                "month": month_str,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "statistics": {
                    "time_controls_processed": successful + failed,
                    "time_controls_successful": successful,
                    "time_controls_failed": failed,
                    "details": {tc: {"processed": processed, "successful": success_count} 
                              for tc, processed, success_count in success_details}
                },
                "step": "calculation_scraping",
                "method": "high_concurrency_batch"
            }
            
            local_file = os.path.join(self.local_temp_dir, f"calculation_scraping_completion_{month_str}.json")
            async with aiofiles.open(local_file, "w") as f:
                await f.write(json.dumps(completion_data, indent=2))
            
            s3_key = f"results/{month_str}/calculation_scraping_completion.json"
            await self._upload_to_s3_async(local_file, s3_key)
            
            os.remove(local_file)
            logger.info(f"Uploaded completion marker for {month_str}")
            
        except Exception as e:
            logger.warning(f"Failed to upload completion marker: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Scrape FIDE player calculation data.")
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
    
    logger.info(f"Starting calculation scraper for {args.month}")
    
    # Validate month format
    try:
        datetime.strptime(args.month, "%Y-%m")
    except ValueError:
        logger.error("Month must be in YYYY-MM format")
        return 1
    
    # Initialize scraper
    scraper = PlayerCalculationScraper(
        s3_bucket=args.s3_bucket,
        aws_region=args.aws_region
    )
    
    # Run scraping
    try:
        success = asyncio.run(scraper.scrape_calculations_for_month(args.month))
        
        if success:
            logger.info(f"Calculation scraping completed successfully for {args.month}")
            return 0
        else:
            logger.error("Calculation scraping failed")
            return 1
            
    except Exception as e:
        logger.error(f"Calculation scraping failed with error: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    exit(main())