import os
import json
import argparse
import asyncio
import aiofiles
import boto3
import logging
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
        
        # Semaphore for controlling S3 download concurrency
        self.download_semaphore = asyncio.Semaphore(20)
        
        logger.info(f"Initialized player ID aggregator - S3 bucket: {s3_bucket}, Region: {aws_region}")

    async def aggregate_players_for_month(self, month_str):
        """Aggregate player IDs for all time controls for a specific month"""
        try:
            year, month = map(int, month_str.split("-"))
            logger.info(f"Starting player aggregation for {year}-{month:02d}")
            
            # Setup local temp directory
            os.makedirs(self.local_temp_dir, exist_ok=True)
            
            # Time control categories
            time_controls = ["standard", "rapid", "blitz"]
            
            # Initialize thread-safe sets and locks for each time control
            for tc in time_controls:
                self.player_sets[tc] = set()
                self.set_locks[tc] = threading.Lock()
            
            # Process each time control in parallel
            tasks = [
                self._process_time_control(tc, year, month)
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
                    logger.info(f"No tournaments found for {tc}")
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

    async def _process_time_control(self, time_control: str, year: int, month: int):
        """Process all tournaments for a specific time control"""
        month_str = f"{month:02d}"
        
        try:
            # List all tournament files for this time control and month
            tournament_files = await self._list_tournament_files(time_control, year, month)
            
            if not tournament_files:
                logger.info(f"No tournament files found for {time_control} {year}-{month_str}")
                return "skipped"
            
            logger.info(f"Found {len(tournament_files)} tournament files for {time_control}")
            
            # Process tournaments in chunks to manage memory and concurrency
            chunk_size = 50  # Process 50 tournaments at a time
            tournaments_processed = 0
            
            for i in range(0, len(tournament_files), chunk_size):
                chunk = tournament_files[i:i + chunk_size]
                
                # Process this chunk of tournaments in parallel
                tasks = [
                    self._process_tournament_file(s3_key, time_control)
                    for s3_key in chunk
                ]
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Count successful processes
                successful_in_chunk = sum(1 for result in results if result is True)
                tournaments_processed += successful_in_chunk
                
                logger.info(f"{time_control}: Processed {tournaments_processed}/{len(tournament_files)} tournaments, "
                          f"{len(self.player_sets[time_control])} unique players found so far")
            
            unique_player_count = len(self.player_sets[time_control])
            logger.info(f"{time_control}: Completed processing {tournaments_processed} tournaments, "
                       f"found {unique_player_count} unique players")
            
            return ("success", unique_player_count)
            
        except Exception as e:
            logger.error(f"Error processing time control {time_control}: {str(e)}")
            return e

    async def _list_tournament_files(self, time_control: str, year: int, month: int) -> List[str]:
        """List all tournament files for a specific time control and month from S3"""
        month_str = f"{month:02d}"
        prefix = f"persistent/tournament_data/processed/{time_control}/{year}-{month_str}/"
        
        try:
            loop = asyncio.get_event_loop()
            
            # Use paginator to handle large numbers of files
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.s3_bucket, Prefix=prefix)
            
            tournament_files = []
            
            # Run pagination in executor to avoid blocking
            for page in await loop.run_in_executor(None, lambda: list(page_iterator)):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if key.endswith('.txt'):
                            tournament_files.append(key)
            
            return tournament_files
            
        except ClientError as e:
            logger.error(f"Error listing tournament files for {time_control}: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error listing tournament files for {time_control}: {str(e)}")
            return []

    async def _process_tournament_file(self, s3_key: str, time_control: str) -> bool:
        """Download and process a single tournament file"""
        try:
            async with self.download_semaphore:
                # Download tournament file from S3
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
                )
                
                content = response['Body'].read().decode('utf-8')
                
                # Parse player IDs from the file
                player_ids = set()
                for line in content.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        player_data = json.loads(line)
                        if "id" in player_data:
                            player_ids.add(player_data["id"])
                    except json.JSONDecodeError:
                        logger.warning(f"Invalid JSON in {s3_key}: {line}")
                        continue
                
                # Thread-safe addition to the global set for this time control
                with self.set_locks[time_control]:
                    self.player_sets[time_control].update(player_ids)
                
                return True
                
        except ClientError as e:
            logger.warning(f"Error downloading {s3_key}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error processing {s3_key}: {str(e)}")
            return False

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
                "method": "parallel_s3"
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
    parser = argparse.ArgumentParser(description="Aggregate player IDs from processed tournament files.")
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