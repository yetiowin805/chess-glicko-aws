import os
import json
import argparse
import asyncio
import aiohttp
import aiofiles
import boto3
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple, Optional, Dict
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
TOURNAMENT_INFO_PATH = "tournament_information.phtml?event="

class FIDETournamentScraper:
    def __init__(self, s3_bucket, aws_region=DEFAULT_AWS_REGION):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3', region_name=aws_region)
        self.local_temp_dir = "/tmp/chess_data"
        
        # Semaphores for different levels of concurrency
        self.country_semaphore = asyncio.Semaphore(20)  # Max countries processed simultaneously
        self.tournament_semaphore = asyncio.Semaphore(50)  # Max tournaments processed simultaneously
        
        # Tournament data cache for building SQLite databases - now organized by month and time control
        self.tournament_data = {}
        
        # Lock for thread-safe access to tournament_data
        self.data_lock = asyncio.Lock()
        
        logger.info(f"Initialized tournament scraper - S3 bucket: {s3_bucket}, Region: {aws_region}")
        logger.info(f"Total countries to process: {len(countries)}")
        
    def is_valid_rating_period(self, year: int, month: int) -> bool:
        """
        Validate if the given year and month combination is a valid FIDE rating period.
        """
        if year < 2009:
            return month in [1, 4, 7, 10]
        elif year == 2009:
            return (month < 9 and month in [1, 4, 7]) or (month >= 9 and month % 2 == 1)
        elif year < 2012:
            return month % 2 == 1
        elif year == 2012:
            return (month < 8 and month % 2 == 1) or month >= 8
        else:
            return True

    def get_months_in_period(self, year: int, month: int) -> List[str]:
        """Get all individual months covered by a rating period"""
        if year < 2009 or (year == 2009 and month < 9):
            # 3-month periods
            if month == 1:
                return [f"{year-1}-11", f"{year-1}-12", f"{year}-01"]
            elif month == 4:
                return [f"{year}-02", f"{year}-03", f"{year}-04"]
            elif month == 7:
                return [f"{year}-05", f"{year}-06", f"{year}-07"]
            elif month == 10:
                return [f"{year}-08", f"{year}-09", f"{year}-10"]
        elif year < 2012 or (year == 2012 and month < 8):
            # 2-month periods (odd months)
            if month % 2 == 1:  # odd month
                if month == 1:
                    return [f"{year-1}-12", f"{year}-01"]
                if month == 3:
                    return [f"{year}-02", f"{year}-03"]
                if month == 5:
                    return [f"{year}-04", f"{year}-05"]
                if month == 7:
                    return [f"{year}-06", f"{year}-07"]
                if month == 9:
                    return [f"{year}-08", f"{year}-09"]
                if month == 11:
                    return [f"{year}-10", f"{year}-11"]
        else:
            # Monthly periods
            return [f"{year}-{month:02d}"]
        
        return []

    def get_target_month_for_tournament(self, date_received: Optional[str], end_date: Optional[str], 
                                      period_months: List[str]) -> str:
        """
        Determine which month a tournament should be assigned to based on dates and period.
        """
        def parse_date(date_str: str) -> Optional[datetime]:
            if not date_str or date_str == "0000-00-00" or date_str.strip() == "":
                return None
            try:
                return datetime.strptime(date_str.strip(), "%Y-%m-%d")
            except (ValueError, AttributeError):
                return None

        def get_next_month(date_obj: datetime) -> str:
            next_month = date_obj.replace(day=1) + timedelta(days=32)
            return f"{next_month.year}-{next_month.month:02d}"

        def round_to_period(target_month: str, period_months: List[str]) -> str:
            if target_month in period_months:
                return target_month
            
            # Convert to datetime for comparison
            try:
                target_dt = datetime.strptime(target_month + "-01", "%Y-%m-%d")
                period_dts = [datetime.strptime(pm + "-01", "%Y-%m-%d") for pm in period_months]
                
                # If target is before the period, use first month
                if target_dt < min(period_dts):
                    return period_months[0]
                # If target is after the period, use last month
                elif target_dt > max(period_dts):
                    return period_months[-1]
                # Otherwise, find closest month
                else:
                    closest = min(period_dts, key=lambda x: abs((x - target_dt).days))
                    return f"{closest.year}-{closest.month:02d}"
            except:
                return period_months[-1]  # Default to last month if parsing fails

        # Try date received first
        received_dt = parse_date(date_received)
        if received_dt:
            target_month = get_next_month(received_dt)
            return round_to_period(target_month, period_months)
        
        # Fall back to end date
        end_dt = parse_date(end_date)
        if end_dt:
            target_month = get_next_month(end_dt)
            return round_to_period(target_month, period_months)

        # Default to last month of period
        return period_months[-1]

    async def process_tournaments_for_month(self, month_str):
        """Process tournament data for all countries for a specific month"""
        try:
            year, month = map(int, month_str.split("-"))
            logger.info(f"Starting tournament processing for {year}-{month:02d}")
            
            if not self.is_valid_rating_period(year, month):
                logger.warning(f"Invalid rating period: {year}-{month:02d}, skipping")
                return True
            
            # Get all months covered by this period
            period_months = self.get_months_in_period(year, month)
            logger.info(f"Rating period {month_str} covers months: {period_months}")
            
            # Setup local temp directory
            os.makedirs(self.local_temp_dir, exist_ok=True)
            
            # Initialize tournament data structure - now consolidated by time control only
            self.tournament_data = {
                "standard": [],
                "rapid": [],
                "blitz": []
            }
            
            # Check if SQLite databases already exist for all time controls
            all_exist = await self._check_all_databases_exist(month_str, year)
            if all_exist:
                logger.info(f"All tournament databases already exist for period {month_str}")
                return True
            
            # Configure async session with higher limits for combined processing
            connector = aiohttp.TCPConnector(limit=200, limit_per_host=30)
            timeout = aiohttp.ClientTimeout(total=60)
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                # Create tasks for all countries - each country processes completely independently
                tasks = [
                    self._process_country_tournaments(session, country, year, month, period_months)
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
                
                # Create and upload consolidated SQLite databases for each time control
                await self._create_and_upload_databases(month_str, year, month)
                
                # Log failures for debugging
                if failed:
                    logger.error(f"Failed countries: {[f[0] for f in failed[:10]]}")
                    for country, error in failed[:3]:
                        logger.error(f"  {country}: {error}")
                
                # Upload completion marker
                await self._upload_completion_marker(month_str, len(successful), len(failed), len(skipped), total_tournaments, period_months)
                
                return True
                
        except Exception as e:
            logger.error(f"Error in tournament processing: {str(e)}", exc_info=True)
            raise

    async def _check_all_databases_exist(self, month_str: str, year: int) -> bool:
        """Check if consolidated SQLite databases already exist for all time controls"""
        # Determine which time controls should exist based on year
        time_controls_to_check = ["standard"]
        if year >= 2012:
            time_controls_to_check.extend(["rapid", "blitz"])
        
        for time_control in time_controls_to_check:
            s3_key = f"persistent/tournament_data/processed/{time_control}/{month_str}.db"
            if not await self._check_s3_file_exists_async(s3_key):
                return False
        
        return True

    async def _process_country_tournaments(self, session: aiohttp.ClientSession, country: str, year: int, month: int, period_months: List[str]):
        """Process all tournaments for a specific country - fetch tournament list then process each tournament"""
        
        async with self.country_semaphore:
            try:
                # Step 1: Get tournament IDs for this country
                tournament_ids = await self._fetch_tournament_ids(session, country, year, month)
                if not tournament_ids:
                    return "skipped"
                
                logger.info(f"Found {len(tournament_ids)} tournaments for {country}, processing in parallel")
                
                # Step 2: Process all tournaments for this country in parallel
                tasks = [
                    self._process_single_tournament(session, tournament_id, country, year, month, period_months)
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

    async def _fetch_tournament_info(self, session: aiohttp.ClientSession, tournament_id: str) -> Tuple[Optional[str], Optional[str]]:
        """Fetch tournament information to get date received and end date"""
        url = f"{BASE_URL}/{TOURNAMENT_INFO_PATH}{tournament_id}"
        
        try:
            async with session.get(url, timeout=30) as response:
                response.raise_for_status()
                text = await response.text()
                soup = BeautifulSoup(text, "html.parser")
                
                date_received = None
                end_date = None
                
                # Look for the details table
                details_table = soup.find("table", {"class": "details_table"})
                if details_table:
                    for row in details_table.find_all("tr"):
                        cells = row.find_all("td")
                        if len(cells) >= 2:
                            label = cells[0].get_text(strip=True).lower()
                            value = cells[1].get_text(strip=True)
                            
                            if "date received" in label:
                                date_received = value
                            elif "end date" in label:
                                end_date = value
                
                return date_received, end_date
                
        except Exception as e:
            logger.warning(f"Failed to fetch tournament info for {tournament_id}: {str(e)}")
            return None, None

    async def _process_single_tournament(self, session: aiohttp.ClientSession, tournament_id: str, country: str, year: int, month: int, period_months: List[str]) -> bool:
        """Process a single tournament and add player data to cache"""
        url = f"{BASE_URL}/{TOURNAMENT_REPORT_PATH}{tournament_id}"
        
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
                            logger.warning(f"No player data found for tournament {tournament_id}")
                            return True

                        # Map time_control value to directory name
                        time_control_dirs = {"0": "standard", "1": "rapid", "2": "blitz"}
                        tc_dir = time_control_dirs.get(time_control, "standard")

                        # For periods before August 2012, fetch additional tournament info to determine target month
                        target_month = None
                        if year < 2012 or (year == 2012 and month < 8):
                            date_received, end_date = await self._fetch_tournament_info(session, tournament_id)
                            
                            if not date_received and not end_date:
                                logger.warning(f"No date information found for tournament {tournament_id}")
                            
                            target_month = self.get_target_month_for_tournament(date_received, end_date, period_months)
                            logger.debug(f"Tournament {tournament_id}: date_received={date_received}, end_date={end_date}, target_month={target_month}")
                        else:
                            # For monthly periods, use the single month
                            target_month = period_months[0]

                        # Add tournament data to consolidated cache (thread-safe)
                        async with self.data_lock:
                            for player_id, player_name in player_data:
                                self.tournament_data[tc_dir].append({
                                    'tournament_id': tournament_id,
                                    'player_id': player_id,
                                    'player_name': player_name,
                                    'target_month': target_month
                                })
                        
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

    async def _create_and_upload_databases(self, month_str: str, year: int, month: int):
        """Create SQLite databases for each time control and upload to S3"""
        # Determine which time controls to create based on year
        time_controls_to_create = ["standard"]
        if year > 2012 or (year == 2012 and month >= 2):
            time_controls_to_create.extend(["rapid", "blitz"])
        
        for time_control in time_controls_to_create:
            if self.tournament_data[time_control]:
                await self._create_time_control_database(time_control, month_str)
            else:
                logger.info(f"No data found for {time_control} time control")

    async def _create_time_control_database(self, time_control: str, month_str: str):
        """Create SQLite database for a specific time control and month"""
        local_db_path = os.path.join(self.local_temp_dir, f"tournaments_{time_control}_{month_str}.db")
        
        try:
            # Create SQLite database
            conn = sqlite3.connect(local_db_path)
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=NORMAL')
            
            # Create table with target_month column
            conn.execute('''
                CREATE TABLE tournament_players (
                    tournament_id TEXT NOT NULL,
                    player_id TEXT NOT NULL,
                    player_name TEXT NOT NULL,
                    target_month TEXT NOT NULL,
                    PRIMARY KEY (tournament_id, player_id)
                )
            ''')
            
            # Create indexes for fast lookups
            conn.execute('CREATE INDEX idx_tournament_id ON tournament_players(tournament_id)')
            conn.execute('CREATE INDEX idx_player_name ON tournament_players(LOWER(player_name))')
            conn.execute('CREATE INDEX idx_tournament_name ON tournament_players(tournament_id, LOWER(player_name))')
            conn.execute('CREATE INDEX idx_target_month ON tournament_players(target_month)')
            
            # Insert data in batches
            batch_size = 1000
            data_to_insert = [
                (record['tournament_id'], record['player_id'], record['player_name'], record['target_month'])
                for record in self.tournament_data[time_control]
            ]
            
            for i in range(0, len(data_to_insert), batch_size):
                batch = data_to_insert[i:i + batch_size]
                conn.executemany(
                    'INSERT OR REPLACE INTO tournament_players (tournament_id, player_id, player_name, target_month) VALUES (?, ?, ?, ?)',
                    batch
                )
            
            conn.commit()
            conn.close()
            
            logger.info(f"Created {time_control} database for {month_str} with {len(data_to_insert)} records")
            
            # Upload to S3
            s3_key = f"persistent/tournament_data/processed/{time_control}/{month_str}.db"
            await self._upload_to_s3_async(local_db_path, s3_key)
            
            # Cleanup local file
            os.remove(local_db_path)
            
            logger.info(f"Uploaded {time_control} database for {month_str} to {s3_key}")
            
        except Exception as e:
            logger.error(f"Error creating database for {time_control} {month_str}: {str(e)}")
            if os.path.exists(local_db_path):
                os.remove(local_db_path)
            raise

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

    async def _upload_completion_marker(self, month_str: str, successful: int, failed: int, skipped: int, total_tournaments: int, period_months: List[str]):
        """Upload completion marker with statistics"""
        try:
            # Calculate database records per time control
            database_info = {}
            for time_control in ["standard", "rapid", "blitz"]:
                database_info[f"{time_control}_records"] = len(self.tournament_data[time_control])
            
            completion_data = {
                "month": month_str,
                "period_months": period_months,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "statistics": {
                    "countries_processed": len(countries),
                    "countries_successful": successful,
                    "countries_failed": failed,
                    "countries_skipped": skipped,
                    "total_tournaments_processed": total_tournaments
                },
                "database_info": database_info,
                "step": "combined_tournament_processing_multi_month_consolidated",
                "method": "aiohttp_parallel_sqlite_consolidated"
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
    parser = argparse.ArgumentParser(description="Scrape and process FIDE tournament data into SQLite databases with multi-month support.")
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
    
    logger.info(f"Starting enhanced tournament scraper for {args.month}")
    
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
            logger.info(f"Enhanced tournament processing completed successfully for {args.month}")
            return 0
        else:
            logger.error("Enhanced tournament processing failed")
            return 1
            
    except Exception as e:
        logger.error(f"Enhanced tournament processing failed with error: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    exit(main())