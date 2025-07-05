import os
import json
import argparse
import asyncio
import aiofiles
import boto3
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Set, List
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor
import threading

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_AWS_REGION = "us-east-2"

class PlayerIDAggregator:
    def __init__(self, s3_bucket, aws_region=DEFAULT_AWS_REGION):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3', region_name=aws_region)
        self.local_temp_dir = "/tmp/chess_data"
        
        # Thread-safe set with lock for each time control
        self.player_sets = {}
        self.set_locks = {}
        
        logger.info(f"Initialized player ID aggregator - S3 bucket: {s3_bucket}, Region: {aws_region}")

    async def aggregate_players_for_month(self, month_str):
        """Aggregate player IDs for all time controls for a specific month"""
        try:
            year, month = map(int, month_str.split("-"))
            logger.info(f"Starting player aggregation for {year}-{month:02d}")
            
            # Setup local temp directory
            os.makedirs(self.local_temp_dir, exist_ok=True)
            
            # Determine which time controls to process based on year
            time_controls = ["standard"]
            if year >= 2012:
                time_controls.extend(["rapid", "blitz"])
            
            # Initialize thread-safe sets and locks for each time control
            for tc in time_controls:
                self.player_sets[tc] = set()
                self.set_locks[tc] = threading.Lock()
            
            # Process each time control in parallel
            tasks = [
                self._process_time_control(tc, month_str)
                for tc in time_controls
            ]
            
            logger.info(f"Processing {len(time_controls)} time controls in parallel")
            
            # Execute all time control tasks concurrently
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results and save aggregated player lists
            successful = []
            failed = []
            
            for i, result in enumerate(results):
                tc = time_controls[i]
                if isinstance(result, tuple) and result[0] == "success":
                    player_count = result[1]
                    successful.append((tc, player_count))
                    
                    # Save aggregated player list for this time control
                    await self._save_player_list(tc, month_str, self.player_sets[tc])
                    
                elif result == "skipped":
                    logger.info(f"No tournament database found for {tc}")
                else:
                    failed.append((tc, str(result) if isinstance(result, Exception) else result))
            
            total_unique_players = sum(count for _, count in successful)
            logger.info(f"Results - Time controls: {len(successful)} successful, {len(failed)} failed")
            logger.info(f"Total unique players across all time controls: {total_unique_players}")
            
            # Log failures for debugging
            if failed:
                logger.error(f"Failed time controls: {[f[0] for f in failed]}")
                for tc, error in failed:
                    logger.error(f"  {tc}: {error}")
            
            # Upload completion marker
            await self._upload_completion_marker(month_str, len(successful), len(failed), successful)
            
            return True
            
        except Exception as e:
            logger.error(f"Error in player aggregation: {str(e)}", exc_info=True)
            raise

    async def _process_time_control(self, time_control: str, month_str: str):
        """Process tournament database for a specific time control"""
        try:
            # Check if tournament database exists for this time control
            s3_key = f"persistent/tournament_data/processed/{time_control}/{month_str}.db"
            
            if not await self._check_s3_file_exists_async(s3_key):
                logger.info(f"No tournament database found for {time_control} {month_str}")
                return "skipped"
            
            logger.info(f"Found tournament database for {time_control}, downloading and processing...")
            
            # Download the SQLite database
            local_db_path = await self._download_tournament_database(s3_key, time_control, month_str)
            
            if not local_db_path:
                logger.error(f"Failed to download tournament database for {time_control}")
                return Exception(f"Failed to download database for {time_control}")
            
            # Extract player IDs from the database
            player_count = await self._extract_player_ids_from_database(local_db_path, time_control)
            
            # Clean up local database file
            if os.path.exists(local_db_path):
                os.remove(local_db_path)
            
            logger.info(f"{time_control}: Found {player_count} unique players")
            
            return ("success", player_count)
            
        except Exception as e:
            logger.error(f"Error processing time control {time_control}: {str(e)}")
            return e

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

    async def _download_tournament_database(self, s3_key: str, time_control: str, month_str: str) -> str:
        """Download tournament SQLite database from S3"""
        local_db_path = os.path.join(self.local_temp_dir, f"tournaments_{time_control}_{month_str}.db")
        
        try:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self.s3_client.download_file(self.s3_bucket, s3_key, local_db_path)
            )
            
            logger.info(f"Downloaded tournament database for {time_control}")
            return local_db_path
            
        except ClientError as e:
            logger.error(f"Error downloading tournament database {s3_key}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error downloading tournament database {s3_key}: {str(e)}")
            return None

    async def _extract_player_ids_from_database(self, db_path: str, time_control: str) -> int:
        """Extract unique player IDs from tournament SQLite database"""
        try:
            loop = asyncio.get_event_loop()
            
            # Run database query in executor to avoid blocking
            def query_database():
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # Get all unique player IDs from the database
                cursor.execute("SELECT DISTINCT player_id FROM tournament_players")
                player_ids = {row[0] for row in cursor.fetchall()}
                
                # Get some statistics for logging
                cursor.execute("SELECT COUNT(*) FROM tournament_players")
                total_records = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(DISTINCT tournament_id) FROM tournament_players")
                unique_tournaments = cursor.fetchone()[0]
                
                conn.close()
                
                logger.info(f"{time_control}: Database contains {total_records} records across {unique_tournaments} tournaments")
                
                return player_ids
            
            # Execute database query
            player_ids = await loop.run_in_executor(None, query_database)
            
            # Thread-safe addition to the global set for this time control
            with self.set_locks[time_control]:
                self.player_sets[time_control].update(player_ids)
            
            return len(player_ids)
            
        except Exception as e:
            logger.error(f"Error extracting player IDs from database {db_path}: {str(e)}")
            raise

    async def _save_player_list(self, time_control: str, month_str: str, player_ids: Set[str]):
        """Save aggregated player list to local file and upload to S3"""
        try:
            if not player_ids:
                logger.info(f"No player IDs found for {time_control} in {month_str}")
                return
            
            # Sort the IDs numerically
            sorted_ids = sorted(player_ids, key=int)
            
            # Save to local file
            local_dir = os.path.join(self.local_temp_dir, "active_players")
            os.makedirs(local_dir, exist_ok=True)
            
            local_file = os.path.join(local_dir, f"{month_str}_{time_control}.txt")
            
            async with aiofiles.open(local_file, "w", encoding="utf-8") as f:
                await f.write("\n".join(sorted_ids))
            
            # Upload to S3
            s3_key = f"persistent/active_players/{month_str}_{time_control}.txt"
            await self._upload_to_s3_async(local_file, s3_key)
            
            # Cleanup local file
            os.remove(local_file)
            
            logger.info(f"Successfully saved {len(sorted_ids)} unique player IDs for {time_control} to S3")
            
        except Exception as e:
            logger.error(f"Error saving player list for {time_control}: {str(e)}")
            raise

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
                    "details": {tc: count for tc, count in success_details}
                },
                "step": "player_aggregation",
                "method": "sqlite_database"
            }
            
            local_file = os.path.join(self.local_temp_dir, f"player_aggregation_completion_{month_str}.json")
            async with aiofiles.open(local_file, "w") as f:
                await f.write(json.dumps(completion_data, indent=2))
            
            s3_key = f"results/{month_str}/player_aggregation_completion.json"
            await self._upload_to_s3_async(local_file, s3_key)
            
            os.remove(local_file)
            logger.info(f"Uploaded completion marker for {month_str}")
            
        except Exception as e:
            logger.warning(f"Failed to upload completion marker: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Aggregate player IDs from processed tournament databases.")
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
    
    logger.info(f"Starting player ID aggregator for {args.month}")
    
    # Validate month format
    try:
        datetime.strptime(args.month, "%Y-%m")
    except ValueError:
        logger.error("Month must be in YYYY-MM format")
        return 1
    
    # Initialize aggregator
    aggregator = PlayerIDAggregator(
        s3_bucket=args.s3_bucket,
        aws_region=args.aws_region
    )
    
    # Run aggregation
    try:
        success = asyncio.run(aggregator.aggregate_players_for_month(args.month))
        
        if success:
            logger.info(f"Player ID aggregation completed successfully for {args.month}")
            return 0
        else:
            logger.error("Player ID aggregation failed")
            return 1
            
    except Exception as e:
        logger.error(f"Player ID aggregation failed with error: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    exit(main())