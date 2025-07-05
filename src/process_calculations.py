import os
import json
import argparse
import asyncio
import aiofiles
import boto3
import logging
import sqlite3
from datetime import datetime
from typing import Dict, List, Set, Optional, Tuple
from collections import defaultdict
from botocore.exceptions import ClientError
from pathlib import Path
import gzip
import tempfile
import string
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_AWS_REGION = "us-east-2"
TIME_CONTROLS = ["standard", "rapid", "blitz"]

class CalculationProcessor:
    def __init__(self, s3_bucket: str, aws_region: str = DEFAULT_AWS_REGION, month_str: str = None):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3', region_name=aws_region)
        self.local_temp_dir = "/tmp/chess_data"
        self.month_str = month_str
        # Cache for SQLite database connections
        self.db_connections = {}  # time_control -> connection
        
        logger.info(f"Initialized calculation processor - S3 bucket: {s3_bucket}, Region: {aws_region}")

    async def process_calculations_for_month(self):
        """Process calculation data for all time controls for a specific month"""
        try:
            logger.info(f"Starting calculation processing for {self.month_str}")
            
            # Setup local temp directory
            os.makedirs(self.local_temp_dir, exist_ok=True)
            
            # Parse year and month from month_str
            year, month = map(int, self.month_str.split("-"))
            
            # Determine which time controls to process based on year
            time_controls_to_process = ["standard"]
            if year >= 2012:
                time_controls_to_process.extend(["rapid", "blitz"])
            
            logger.info(f"Processing time controls: {time_controls_to_process}")
            
            # Process each time control
            results = []
            for time_control in time_controls_to_process:
                try:
                    # Load SQLite database for this time control
                    await self._load_player_database(time_control)
                    
                    result = await self._process_time_control(time_control)
                    results.append((time_control, result))
                    
                    # Close database connection for this time control
                    await self._close_database_connection(time_control)
                    
                except Exception as e:
                    logger.error(f"Error processing {time_control}: {str(e)}")
                    results.append((time_control, {"error": str(e)}))
            
            # Generate summary and upload completion marker
            await self._upload_processing_completion_marker(results)
            
            logger.info(f"Calculation processing completed for {self.month_str}")
            return True
            
        except Exception as e:
            logger.error(f"Error in calculation processing: {str(e)}", exc_info=True)
            raise
        finally:
            # Ensure all database connections are closed
            await self._cleanup_database_connections()

    async def _load_player_database(self, time_control: str):
        """Download and load SQLite database for player lookups"""
        s3_key = f"persistent/player_info/processed/{time_control}/{self.month_str}.db"
        local_db_path = os.path.join(self.local_temp_dir, f"{self.month_str}_{time_control}.db")
        
        try:
            logger.info(f"Downloading player database for {time_control}...")
            
            # Download SQLite database from S3
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.s3_client.download_file(self.s3_bucket, s3_key, local_db_path)
            )
            
            # Open database connection
            conn = sqlite3.connect(local_db_path)
            conn.row_factory = sqlite3.Row  # Return rows as dictionaries
            
            # Test the connection and get player count
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM players")
            player_count = cursor.fetchone()[0]
            
            self.db_connections[time_control] = conn
            
            # Pre-load all player mappings into memory
            await self._preload_player_mappings(time_control)
            
            logger.info(f"Loaded player database for {time_control} with {player_count} players")
            
        except ClientError as e:
            logger.error(f"Failed to download player database for {time_control}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error loading player database for {time_control}: {str(e)}")
            raise

    async def _preload_player_mappings(self, time_control: str):
        """Pre-load all player name->ID mappings into memory for fast lookups"""
        if not hasattr(self, 'player_mappings'):
            self.player_mappings = {}
            self.player_mappings_normalized = {}
        
        conn = self.db_connections[time_control]
        cursor = conn.cursor()
        
        logger.info(f"Pre-loading player mappings for {time_control}...")
        
        # Load all players with their names
        cursor.execute("SELECT id, name FROM players")
        
        self.player_mappings[time_control] = {}
        self.player_mappings_normalized[time_control] = {}
        
        for row in cursor.fetchall():
            player_id, name = row
            
            # Store exact lowercase match
            self.player_mappings[time_control][name.lower()] = player_id
            
            # Store normalized version
            normalized = self._normalize_player_name(name)
            if normalized:
                self.player_mappings_normalized[time_control][normalized] = player_id
        
        logger.info(f"Pre-loaded {len(self.player_mappings[time_control])} player mappings for {time_control}")

    async def _close_database_connection(self, time_control: str):
        """Close database connection for a specific time control"""
        if time_control in self.db_connections:
            try:
                self.db_connections[time_control].close()
                del self.db_connections[time_control]
                
                # Also remove the local database file
                local_db_path = os.path.join(self.local_temp_dir, f"{self.month_str}_{time_control}.db")
                if os.path.exists(local_db_path):
                    os.remove(local_db_path)
                    
                logger.debug(f"Closed database connection for {time_control}")
            except Exception as e:
                logger.warning(f"Error closing database connection for {time_control}: {str(e)}")

    async def _cleanup_database_connections(self):
        """Close all database connections and cleanup files"""
        for time_control in list(self.db_connections.keys()):
            await self._close_database_connection(time_control)
        
        # Also cleanup tournament database cache
        if hasattr(self, '_tournament_db_cache'):
            for cache_key, local_path in self._tournament_db_cache.items():
                try:
                    if os.path.exists(local_path):
                        os.remove(local_path)
                        logger.debug(f"Cleaned up cached tournament database: {local_path}")
                except Exception as e:
                    logger.warning(f"Failed to cleanup tournament database {local_path}: {str(e)}")
            self._tournament_db_cache.clear()

    def _normalize_player_name(self, name: str) -> str:
        """Normalize player name: remove parenthesis and content, extra spaces, lowercase, remove punctuation."""
        # Remove parenthesis and content inside
        name_no_paren = re.sub(r'\([^)]*\)', '', name)
        # Remove extra spaces, lowercase, remove punctuation
        normalized = ' '.join(name_no_paren.split()).strip().lower().translate(str.maketrans('', '', string.punctuation))
        return normalized

    async def _process_time_control(self, time_control: str) -> Dict:
        """Process consolidated JSONL calculation file for a specific time control"""
        
        try:
            # Check if already processed
            output_s3_key = f"persistent/calculations_processed/{self.month_str}_{time_control}.json.gz"
            if await self._check_s3_file_exists_async(output_s3_key):
                logger.info(f"Processed calculations already exist for {time_control}: {output_s3_key}")
                return {"status": "already_processed", "output_file": output_s3_key}
            
            # Download the consolidated JSONL file
            jsonl_s3_key = f"consolidated/calculations/{self.month_str}/{time_control}_calculations.jsonl"
            
            if not await self._check_s3_file_exists_async(jsonl_s3_key):
                logger.info(f"No consolidated calculation file found for {time_control}: {jsonl_s3_key}")
                return {"status": "no_data", "files_found": 0}
            
            logger.info(f"Found consolidated calculation file for {time_control}: {jsonl_s3_key}")
            
            # Download and process the JSONL file
            local_jsonl_path = os.path.join(self.local_temp_dir, f"{time_control}_calculations.jsonl")
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.s3_client.download_file(self.s3_bucket, jsonl_s3_key, local_jsonl_path)
            )
            
            # Process the JSONL file
            all_processed_data, total_players, failed_players = await self._process_jsonl_file(local_jsonl_path, time_control)
            
            # Clean up downloaded file
            os.remove(local_jsonl_path)
            
            # Save processed data
            if all_processed_data:
                await self._save_processed_calculations(all_processed_data, time_control)
                
            result = {
                "status": "completed",
                "total_players": total_players,
                "processed_successfully": len(all_processed_data),
                "failed_players": failed_players,
                "output_file": output_s3_key if all_processed_data else None
            }
            
            logger.info(f"{time_control}: Processing completed - {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error processing time control {time_control}: {str(e)}")
            return {"status": "error", "error": str(e)}

    async def _process_jsonl_file(self, jsonl_file_path: str, time_control: str) -> Tuple[List[Dict], int, int]:
        """Process the consolidated JSONL file"""
        processed_data = []
        total_players = 0
        failed_players = 0
        
        try:
            # Read and process each line in the JSONL file
            async with aiofiles.open(jsonl_file_path, 'r', encoding='utf-8') as f:
                async for line in f:
                    line = line.strip()
                    if not line:
                        continue
                        
                    total_players += 1
                    
                    try:
                        # Parse the JSON line
                        player_record = json.loads(line)
                        player_id = player_record.get('player_id')
                        calculation_data = player_record.get('calculation_data', {})
                        
                        if not player_id or not calculation_data:
                            logger.warning(f"Invalid player record: missing player_id or calculation_data")
                            failed_players += 1
                            continue
                        
                        # Process this player's calculation data
                        processed_player = await self._process_single_calculation(calculation_data, player_id, time_control)
                        
                        if processed_player:
                            processed_data.append(processed_player)
                        else:
                            failed_players += 1
                            
                    except json.JSONDecodeError as e:
                        logger.warning(f"Failed to parse JSON line: {e}")
                        failed_players += 1
                        continue
                    except Exception as e:
                        logger.warning(f"Error processing player record: {e}")
                        failed_players += 1
                        continue
                        
            logger.info(f"{time_control}: Processed {total_players} players, {len(processed_data)} successful, {failed_players} failed")
            return processed_data, total_players, failed_players
            
        except Exception as e:
            logger.error(f"Error processing JSONL file {jsonl_file_path}: {str(e)}")
            return [], 0, 0

    async def _process_single_calculation(self, calculation_data: Dict, player_id: str, time_control: str) -> Optional[Dict]:
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
                        logger.warning(f"Invalid result: {result}")
                        continue  # Skip invalid results
                    
                    # Try to get opponent ID from name using SQLite database
                    opponent_id = await self._get_player_id_from_name(opponent_name, time_control)
                    
                    # Parse opponent rating
                    try:
                        opponent_rating_int = int(opponent_rating) if opponent_rating else None
                    except (ValueError, TypeError):
                        opponent_rating_int = None
                        
                    if not opponent_id:
                        # Fallback: try to find opponent in tournament data
                        opponent_id = await self._find_opponent_in_tournament_data(opponent_name, tournament_id, time_control)
                        
                    if not opponent_id:
                        logger.warning(f"Could not resolve opponent ID for '{opponent_name}' in tournament {tournament_id}, player {player_id}")
                        continue  # Skip this game if we can't resolve opponent
                    
                    # Create compact game record
                    game_record = {
                        'tournament_id': tournament_id,
                        'opponent_id': opponent_id,
                        'opponent_rating': opponent_rating_int,
                        'result': score,
                        'player_unrated': is_unrated
                    }
                    
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

    async def _get_player_id_from_name(self, name: str, time_control: str) -> Optional[str]:
        """Get player ID from name using in-memory mappings (much faster)"""
        if not name or time_control not in self.player_mappings:
            return None
        
        # Try exact match first (case-insensitive)
        exact_match = self.player_mappings[time_control].get(name.lower())
        if exact_match:
            return exact_match
        
        # Try normalized match
        normalized_name = self._normalize_player_name(name)
        if normalized_name and time_control in self.player_mappings_normalized:
            normalized_match = self.player_mappings_normalized[time_control].get(normalized_name)
            if normalized_match:
                return normalized_match
        
        return None

    async def _find_opponent_in_tournament_data(self, opponent_name: str, tournament_id: str, time_control: str) -> Optional[str]:
        """Fallback method to find opponent in tournament SQLite database"""
        try:
            # Download tournament database if not already cached
            tournament_db_path = await self._get_tournament_database(time_control)
            
            if not tournament_db_path:
                logger.debug(f"Could not access tournament database for {time_control}")
                return None
            
            # Query the tournament database for this specific tournament and opponent name
            conn = sqlite3.connect(tournament_db_path)
            cursor = conn.cursor()
            
            # Try exact match first (case-insensitive)
            cursor.execute("""
                SELECT player_id FROM tournament_players 
                WHERE tournament_id = ? AND LOWER(player_name) = LOWER(?)
                LIMIT 1
            """, (tournament_id, opponent_name))
            
            result = cursor.fetchone()
            if result:
                conn.close()
                logger.debug(f"Found opponent '{opponent_name}' in tournament {tournament_id} via exact match")
                return result[0]

            conn.close()
            
            logger.warning(f"No match found for '{opponent_name}' in tournament {tournament_id}")
            return None
            
        except Exception as e:
            logger.warning(f"Could not find opponent '{opponent_name}' in tournament database for {tournament_id}: {str(e)}")
            return None

    async def _get_tournament_database(self, time_control: str) -> Optional[str]:
        """Download and cache tournament database for fallback lookups"""
        cache_key = f"tournament_{time_control}"
        
        # Check if already cached
        if hasattr(self, '_tournament_db_cache') and cache_key in self._tournament_db_cache:
            local_path = self._tournament_db_cache[cache_key]
            if os.path.exists(local_path):
                return local_path
        
        # Initialize cache if not exists
        if not hasattr(self, '_tournament_db_cache'):
            self._tournament_db_cache = {}
        
        # Download tournament database
        s3_key = f"persistent/tournament_data/processed/{time_control}/{self.month_str}.db"
        local_path = os.path.join(self.local_temp_dir, f"tournaments_{time_control}_{self.month_str}.db")
        
        try:
            await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: self.s3_client.download_file(self.s3_bucket, s3_key, local_path)
            )
            
            # Cache the path
            self._tournament_db_cache[cache_key] = local_path
            logger.debug(f"Downloaded and cached tournament database for {time_control}")
            
            return local_path
            
        except ClientError as e:
            logger.debug(f"Failed to download tournament database {s3_key}: {str(e)}")
            return None

    async def _save_processed_calculations(self, processed_data: List[Dict], time_control: str):
        """Save processed calculation data to S3"""
        
        # Create local compressed file
        local_file = os.path.join(self.local_temp_dir, f"calculations_processed_{self.month_str}_{time_control}.json.gz")
        
        with gzip.open(local_file, 'wt', encoding='utf-8') as f:
            for record in processed_data:
                f.write(json.dumps(record) + '\n')
        
        # Upload to S3
        s3_key = f"persistent/calculations_processed/{self.month_str}_{time_control}.json.gz"
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

    async def _upload_processing_completion_marker(self, results: List[Tuple[str, Dict]]):
        """Upload completion marker with processing statistics"""
        try:
            completion_data = {
                "month": self.month_str,
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "step": "calculation_processing",
                "input_format": "consolidated_jsonl",
                "results": {}
            }
            
            total_processed = 0
            total_players = 0
            
            for time_control, result in results:
                completion_data["results"][time_control] = result
                
                if result.get("status") == "completed":
                    total_processed += result.get("processed_successfully", 0)
                    total_players += result.get("total_players", 0)
            
            completion_data["summary"] = {
                "total_players_processed": total_players,
                "total_calculations_processed": total_processed,
                "time_controls": len(results)
            }
            
            # Save locally first
            local_file = os.path.join(self.local_temp_dir, f"calculation_processing_completion_{self.month_str}.json")
            async with aiofiles.open(local_file, "w") as f:
                await f.write(json.dumps(completion_data, indent=2))
            
            # Upload to S3
            s3_key = f"results/{self.month_str}/calculation_processing_completion.json"
            await self._upload_to_s3_async(local_file, s3_key)
            
            # Cleanup
            os.remove(local_file)
            
            logger.info(f"Uploaded processing completion marker for {self.month_str}")
            
        except Exception as e:
            logger.warning(f"Failed to upload processing completion marker: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Process FIDE calculation data from consolidated JSONL format.")
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
        aws_region=args.aws_region,
        month_str=args.month
    )
    
    try:
        success = asyncio.run(processor.process_calculations_for_month())
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