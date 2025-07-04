import os
import json
import argparse
import asyncio
import aiofiles
import boto3
import logging
from datetime import datetime
from typing import Dict, List, Set, Optional, Tuple
from collections import defaultdict
from botocore.exceptions import ClientError
from pathlib import Path
import gzip
import tempfile

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_AWS_REGION = "us-east-2"
TIME_CONTROLS = ["standard", "rapid", "blitz"]

class CalculationProcessor:
    def __init__(self, s3_bucket: str, aws_region: str = DEFAULT_AWS_REGION):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3', region_name=aws_region)
        self.local_temp_dir = "/tmp/chess_data"
        
        # For processing efficiency
        self.name_to_id_cache = {}  # Global cache for name->ID lookups
        self.batch_size = 100  # Process files in batches
        
        logger.info(f"Initialized calculation processor - S3 bucket: {s3_bucket}, Region: {aws_region}")

    async def process_calculations_for_month(self, month_str: str):
        """Process calculation data for all time controls for a specific month"""
        try:
            year, month = map(int, month_str.split("-"))
            logger.info(f"Starting calculation processing for {year}-{month:02d}")
            
            # Setup local temp directory
            os.makedirs(self.local_temp_dir, exist_ok=True)
            
            # Load name-to-ID mapping from processed player data
            await self._load_name_to_id_mapping(year, month)
            
            # Determine which time controls to process based on year
            time_controls_to_process = ["standard"]
            if year >= 2012:
                time_controls_to_process.extend(["rapid", "blitz"])
            
            logger.info(f"Processing time controls: {time_controls_to_process}")
            
            # Process each time control
            results = []
            for time_control in time_controls_to_process:
                try:
                    result = await self._process_time_control(time_control, year, month)
                    results.append((time_control, result))
                except Exception as e:
                    logger.error(f"Error processing {time_control}: {str(e)}")
                    results.append((time_control, {"error": str(e)}))
            
            # Generate summary and upload completion marker
            await self._upload_processing_completion_marker(month_str, results)
            
            logger.info(f"Calculation processing completed for {month_str}")
            return True
            
        except Exception as e:
            logger.error(f"Error in calculation processing: {str(e)}", exc_info=True)
            raise

    async def _load_name_to_id_mapping(self, year: int, month: int):
        """Load name-to-ID mapping from processed player data"""
        month_str = f"{month:02d}"
        s3_key = f"persistent/player_info/processed/{year}-{month_str}.txt"
        
        try:
            logger.info("Loading name-to-ID mapping from processed player data...")
            
            # Download processed player data
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            )
            
            content = response['Body'].read().decode('utf-8')
            
            # Parse each line and build mapping
            count = 0
            for line in content.strip().split('\n'):
                if line.strip():
                    try:
                        player_data = json.loads(line)
                        player_id = player_data.get('id', '').strip()
                        player_name = player_data.get('name', '').strip()
                        
                        if player_id and player_name:
                            # Normalize name for lookup (remove extra spaces, handle case)
                            normalized_name = ' '.join(player_name.split()).strip()
                            self.name_to_id_cache[normalized_name] = player_id
                            count += 1
                            
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse player data line: {line[:100]}...")
                        continue
            
            logger.info(f"Loaded {count} name-to-ID mappings")
            
        except ClientError as e:
            logger.error(f"Failed to load player data for name-to-ID mapping: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error loading name-to-ID mapping: {str(e)}")
            raise

    async def _process_time_control(self, time_control: str, year: int, month: int) -> Dict:
        """Process all calculation files for a specific time control"""
        month_str = f"{month:02d}"
        
        try:
            # Check if already processed
            output_s3_key = f"persistent/calculations_processed/{year}-{month_str}_{time_control}.json.gz"
            if await self._check_s3_file_exists_async(output_s3_key):
                logger.info(f"Processed calculations already exist for {time_control}: {output_s3_key}")
                return {"status": "already_processed", "output_file": output_s3_key}
            
            # List all calculation files for this time control
            calculation_files = await self._list_calculation_files(time_control, year, month)
            
            if not calculation_files:
                logger.info(f"No calculation files found for {time_control}")
                return {"status": "no_data", "files_found": 0}
            
            logger.info(f"Found {len(calculation_files)} calculation files for {time_control}")
            
            # Process files in batches
            all_processed_data = []
            failed_files = []
            
            for i in range(0, len(calculation_files), self.batch_size):
                batch = calculation_files[i:i + self.batch_size]
                batch_num = i // self.batch_size + 1
                total_batches = (len(calculation_files) + self.batch_size - 1) // self.batch_size
                
                logger.info(f"{time_control}: Processing batch {batch_num}/{total_batches} ({len(batch)} files)")
                
                batch_data, batch_failed = await self._process_calculation_batch(batch, time_control, year, month)
                
                all_processed_data.extend(batch_data)
                failed_files.extend(batch_failed)
                
                logger.info(f"{time_control}: Batch {batch_num} completed - "
                          f"{len(batch_data)} processed, {len(batch_failed)} failed")
            
            # Save processed data
            if all_processed_data:
                await self._save_processed_calculations(all_processed_data, time_control, year, month)
                
            result = {
                "status": "completed",
                "total_files": len(calculation_files),
                "processed_successfully": len(all_processed_data),
                "failed_files": len(failed_files),
                "output_file": output_s3_key if all_processed_data else None
            }
            
            logger.info(f"{time_control}: Processing completed - {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error processing time control {time_control}: {str(e)}")
            return {"status": "error", "error": str(e)}

    async def _list_calculation_files(self, time_control: str, year: int, month: int) -> List[str]:
        """List all calculation files for a time control from S3"""
        month_str = f"{month:02d}"
        prefix = f"persistent/calculations/{year}-{month_str}/{time_control}/"
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            files = []
            
            async for page in self._paginate_async(paginator, Bucket=self.s3_bucket, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if key.endswith('.json') and key != prefix:  # Exclude directory marker
                            files.append(key)
            
            return files
            
        except Exception as e:
            logger.error(f"Error listing calculation files for {time_control}: {str(e)}")
            return []

    async def _paginate_async(self, paginator, **kwargs):
        """Async wrapper for S3 pagination"""
        loop = asyncio.get_event_loop()
        page_iterator = paginator.paginate(**kwargs)
        
        for page in page_iterator:
            yield await loop.run_in_executor(None, lambda: page)

    async def _process_calculation_batch(self, file_keys: List[str], time_control: str, year: int, month: int) -> Tuple[List[Dict], List[str]]:
        """Process a batch of calculation files"""
        processed_data = []
        failed_files = []
        
        # Download all files in batch
        download_tasks = [self._download_calculation_file(key) for key in file_keys]
        download_results = await asyncio.gather(*download_tasks, return_exceptions=True)
        
        # Process each file
        for i, result in enumerate(download_results):
            file_key = file_keys[i]
            
            if isinstance(result, Exception):
                logger.warning(f"Failed to download {file_key}: {result}")
                failed_files.append(file_key)
                continue
            
            try:
                # Extract player ID from file path
                player_id = file_key.split('/')[-1].replace('.json', '')
                
                # Process the calculation data
                processed_calc = self._process_single_calculation(result, player_id, time_control)
                
                if processed_calc:
                    processed_data.append(processed_calc)
                    
            except Exception as e:
                logger.warning(f"Failed to process {file_key}: {str(e)}")
                failed_files.append(file_key)
        
        return processed_data, failed_files

    async def _download_calculation_file(self, s3_key: str) -> Optional[Dict]:
        """Download and parse a single calculation file"""
        try:
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.s3_client.get_object(Bucket=self.s3_bucket, Key=s3_key)
            )
            
            content = response['Body'].read().decode('utf-8')
            return json.loads(content)
            
        except Exception as e:
            logger.warning(f"Error downloading {s3_key}: {str(e)}")
            return None

    def _process_single_calculation(self, calculation_data: Dict, player_id: str, time_control: str) -> Optional[Dict]:
        """Process a single player's calculation data into compact format"""
        try:
            if not calculation_data.get('tournaments'):
                return None
            
            processed_games = []
            
            for tournament in calculation_data['tournaments']:
                tournament_id = tournament.get('tournament_id', '')
                is_unrated = tournament.get('player_is_unrated', False)
                
                for game in tournament.get('games', []):
                    # Get opponent info
                    opponent_name = game.get('opponent_name', '').strip()
                    opponent_rating = game.get('opponent_rating', '')
                    result = game.get('result', '').strip()
                    
                    # Convert result to numeric score
                    score = self._convert_result_to_score(result)
                    if score is None:
                        continue  # Skip invalid results
                    
                    # Try to get opponent ID from name
                    opponent_id = self._get_player_id_from_name(opponent_name)
                    
                    # Parse opponent rating
                    try:
                        opponent_rating_int = int(opponent_rating) if opponent_rating else None
                    except (ValueError, TypeError):
                        opponent_rating_int = None
                    
                    # Create compact game record
                    game_record = {
                        'tournament_id': tournament_id,
                        'opponent_id': opponent_id,
                        'opponent_name': opponent_name,
                        'opponent_rating': opponent_rating_int,
                        'result': score,
                        'player_unrated': is_unrated
                    }
                    
                    # Only include opponent_name if we couldn't resolve ID
                    if opponent_id:
                        del game_record['opponent_name']
                    else:
                        logger.warning(f"Could not resolve opponent ID for {opponent_name}; {tournament_id}, {player_id}")
                    
                    processed_games.append(game_record)
            
            if not processed_games:
                return None
            
            return {
                'player_id': player_id,
                'time_control': time_control,
                'games': processed_games
            }
            
        except Exception as e:
            logger.warning(f"Error processing calculation for player {player_id}: {str(e)}")
            return None

    def _convert_result_to_score(self, result: str) -> Optional[float]:
        """Convert game result string to numeric score"""
        result = result.strip().lower()
        
        if result in ['1', '1.0', '1.00']:
            return 1.0
        elif result in ['0', '0.0', '0.00']:
            return 0.0
        elif result in ['0.5', '0.50']:
            return 0.5
        else:
            logger.warning(f"Unknown result format: {result}")
            return None

    def _get_player_id_from_name(self, name: str) -> Optional[str]:
        """Get player ID from name using the cache"""
        if not name:
            return None
        
        # Normalize name for lookup
        normalized_name = ' '.join(name.split()).strip()
        
        return self.name_to_id_cache.get(normalized_name)

    async def _save_processed_calculations(self, processed_data: List[Dict], time_control: str, year: int, month: int):
        """Save processed calculation data to S3"""
        month_str = f"{month:02d}"
        
        # Create local compressed file
        local_file = os.path.join(self.local_temp_dir, f"calculations_processed_{year}-{month_str}_{time_control}.json.gz")
        
        with gzip.open(local_file, 'wt', encoding='utf-8') as f:
            for record in processed_data:
                f.write(json.dumps(record) + '\n')
        
        # Upload to S3
        s3_key = f"persistent/calculations_processed/{year}-{month_str}_{time_control}.json.gz"
        await self._upload_to_s3_async(local_file, s3_key)
        
        # Cleanup local file
        os.remove(local_file)
        
        logger.info(f"Saved {len(processed_data)} processed calculations to {s3_key}")

    async def _check_s3_file_exists_async(self, s3_key: str) -> bool:
        """Async wrapper for checking S3 file existence"""
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, 
                lambda: self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_key)
            )
            return True
        except ClientError:
            return False

    async def _upload_to_s3_async(self, local_path: str, s3_key: str):
        """Async wrapper for S3 upload"""
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, 
                lambda: self.s3_client.upload_file(local_path, self.s3_bucket, s3_key)
            )
            logger.info(f"Uploaded {local_path} to {s3_key}")
        except ClientError as e:
            logger.error(f"Failed to upload to S3: {str(e)}")
            raise

    async def _upload_processing_completion_marker(self, month_str: str, results: List[Tuple[str, Dict]]):
        """Upload completion marker with processing statistics"""
        try:
            completion_data = {
                "month": month_str,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "step": "calculation_processing",
                "results": {}
            }
            
            total_processed = 0
            total_files = 0
            
            for time_control, result in results:
                completion_data["results"][time_control] = result
                
                if result.get("status") == "completed":
                    total_processed += result.get("processed_successfully", 0)
                    total_files += result.get("total_files", 0)
            
            completion_data["summary"] = {
                "total_files_processed": total_files,
                "total_calculations_processed": total_processed,
                "time_controls": len(results)
            }
            
            # Save locally first
            local_file = os.path.join(self.local_temp_dir, f"calculation_processing_completion_{month_str}.json")
            async with aiofiles.open(local_file, "w") as f:
                await f.write(json.dumps(completion_data, indent=2))
            
            # Upload to S3
            s3_key = f"results/{month_str}/calculation_processing_completion.json"
            await self._upload_to_s3_async(local_file, s3_key)
            
            # Cleanup
            os.remove(local_file)
            
            logger.info(f"Uploaded processing completion marker for {month_str}")
            
        except Exception as e:
            logger.warning(f"Failed to upload processing completion marker: {str(e)}")

    async def create_test_data(self, month_str: str, sample_size: int = 100):
        """Create test data subset for isolated testing of later pipeline stages"""
        try:
            year, month = map(int, month_str.split("-"))
            month_str_formatted = f"{month:02d}"
            
            logger.info(f"Creating test data for {month_str} with sample size {sample_size}")
            
            # Create test data directory structure
            test_prefix = f"test_data/{month_str}/"
            
            # Sample player data
            await self._create_test_player_data(year, month, sample_size, test_prefix)
            
            # Sample calculation data for each time control
            time_controls = ["standard"]
            if year >= 2012:
                time_controls.extend(["rapid", "blitz"])
            
            for time_control in time_controls:
                await self._create_test_calculation_data(year, month, time_control, sample_size, test_prefix)
            
            # Create test completion marker
            await self._create_test_completion_marker(month_str, time_controls, sample_size)
            
            logger.info(f"Test data created successfully for {month_str}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating test data: {str(e)}")
            return False

    async def _create_test_player_data(self, year: int, month: int, sample_size: int, test_prefix: str):
        """Create sample player data for testing"""
        month_str = f"{month:02d}"
        source_key = f"persistent/player_info/processed/{year}-{month_str}.txt"
        test_key = f"{test_prefix}player_info/processed/{year}-{month_str}.txt"
        
        try:
            # Download full player data
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.s3_client.get_object(Bucket=self.s3_bucket, Key=source_key)
            )
            
            content = response['Body'].read().decode('utf-8')
            lines = content.strip().split('\n')
            
            # Sample lines
            sampled_lines = lines[:sample_size] if len(lines) >= sample_size else lines
            
            # Create local test file
            local_file = os.path.join(self.local_temp_dir, f"test_player_data_{year}-{month_str}.txt")
            async with aiofiles.open(local_file, "w") as f:
                await f.write('\n'.join(sampled_lines))
            
            # Upload test data
            await self._upload_to_s3_async(local_file, test_key)
            
            # Cleanup
            os.remove(local_file)
            
            logger.info(f"Created test player data with {len(sampled_lines)} players")
            
        except Exception as e:
            logger.error(f"Error creating test player data: {str(e)}")

    async def _create_test_calculation_data(self, year: int, month: int, time_control: str, sample_size: int, test_prefix: str):
        """Create sample calculation data for testing"""
        month_str = f"{month:02d}"
        source_prefix = f"persistent/calculations/{year}-{month_str}/{time_control}/"
        test_prefix_calc = f"{test_prefix}calculations/{year}-{month_str}/{time_control}/"
        
        try:
            # List calculation files
            files = await self._list_calculation_files(time_control, year, month)
            
            if not files:
                logger.info(f"No calculation files found for {time_control}")
                return
            
            # Sample files
            sampled_files = files[:sample_size] if len(files) >= sample_size else files
            
            # Copy sampled files
            for file_key in sampled_files:
                file_name = file_key.split('/')[-1]
                test_file_key = f"{test_prefix_calc}{file_name}"
                
                # Copy file within S3
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self.s3_client.copy_object(
                        Bucket=self.s3_bucket,
                        CopySource={'Bucket': self.s3_bucket, 'Key': file_key},
                        Key=test_file_key
                    )
                )
            
            logger.info(f"Created test calculation data for {time_control} with {len(sampled_files)} files")
            
        except Exception as e:
            logger.error(f"Error creating test calculation data for {time_control}: {str(e)}")

    async def _create_test_completion_marker(self, month_str: str, time_controls: List[str], sample_size: int):
        """Create test completion marker"""
        try:
            completion_data = {
                "month": month_str,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "test_data": True,
                "sample_size": sample_size,
                "time_controls": time_controls,
                "note": "This is test data for isolated pipeline testing"
            }
            
            local_file = os.path.join(self.local_temp_dir, f"test_completion_{month_str}.json")
            async with aiofiles.open(local_file, "w") as f:
                await f.write(json.dumps(completion_data, indent=2))
            
            test_key = f"test_data/{month_str}/test_completion.json"
            await self._upload_to_s3_async(local_file, test_key)
            
            os.remove(local_file)
            
            logger.info(f"Created test completion marker for {month_str}")
            
        except Exception as e:
            logger.error(f"Error creating test completion marker: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Process FIDE calculation data into compact format.")
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
    
    # Validate month format
    try:
        datetime.strptime(args.month, "%Y-%m")
    except ValueError:
        logger.error("Month must be in YYYY-MM format")
        return 1
    
    # Initialize processor
    processor = CalculationProcessor(
        s3_bucket=args.s3_bucket,
        aws_region=args.aws_region
    )
    
    try:
        success = asyncio.run(processor.process_calculations_for_month(args.month))
        if success:
            logger.info(f"Calculation processing completed successfully for {args.month}")
            return 0
        else:
            logger.error("Calculation processing failed")
            return 1
                
    except Exception as e:
        logger.error(f"Processing failed with error: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    exit(main())