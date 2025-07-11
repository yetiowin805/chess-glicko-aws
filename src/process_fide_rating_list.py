import os
import json
import argparse
import boto3
import logging
import sqlite3
from datetime import datetime
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_AWS_REGION = "us-east-2"

class FIDEDataProcessor:
    def __init__(self, s3_bucket, aws_region=DEFAULT_AWS_REGION):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3', region_name=aws_region)
        self.local_temp_dir = "/tmp/chess_data"
        
    def process_month_data(self, month_str):
        """Process FIDE player data for a specific month"""
        try:
            year, month = map(int, month_str.split("-"))
            logger.info(f"Starting processing for {year}-{month:02d}")
            
            # Setup local temp directory
            os.makedirs(self.local_temp_dir, exist_ok=True)
            
            # Determine if we need multiple rating lists (after 2012)
            needs_multiple_lists = year > 2012 or (year == 2012 and month >= 9)
            
            if needs_multiple_lists:
                # Process all three rating lists into separate SQLite files
                time_controls = ["standard", "rapid", "blitz"]
                processed_files = []
                
                for time_control in time_controls:
                    # Define S3 keys
                    raw_s3_key = f"persistent/player_info/raw/{time_control}/{year}-{month:02d}.txt"
                    processed_s3_key = f"persistent/player_info/processed/{time_control}/{year}-{month:02d}.db"
                    
                    # Check if processed file already exists in S3
                    if self._check_s3_file_exists(processed_s3_key):
                        logger.info(f"Processed {time_control} file already exists in S3: {processed_s3_key}")
                        processed_files.append(processed_s3_key)
                        continue
                    
                    # Download raw file from S3
                    local_raw_file = os.path.join(self.local_temp_dir, f"{year}-{month:02d}_{time_control}_raw.txt")
                    if not self._download_from_s3(raw_s3_key, local_raw_file):
                        logger.error(f"Failed to download {time_control} raw file from S3: {raw_s3_key}")
                        continue
                    
                    # Process the file
                    local_processed_file = os.path.join(self.local_temp_dir, f"{year}-{month:02d}_{time_control}_processed.db")
                    success = self._process_file(local_raw_file, local_processed_file, year, month, time_control)
                    
                    if success:
                        # Upload processed file to S3
                        self._upload_to_s3(local_processed_file, processed_s3_key)
                        
                        # Cleanup local files
                        self._cleanup_local_files([local_raw_file, local_processed_file])
                        
                        logger.info(f"Successfully processed and uploaded {processed_s3_key}")
                        processed_files.append(processed_s3_key)
                    else:
                        logger.error(f"Processing failed for {time_control}")
                        self._cleanup_local_files([local_raw_file, local_processed_file])
                
                return len(processed_files) > 0
            else:
                # Process only standard rating list (pre-2012 format)
                # Define S3 keys
                raw_s3_key = f"persistent/player_info/raw/standard/{year}-{month:02d}.txt"
                processed_s3_key = f"persistent/player_info/processed/standard/{year}-{month:02d}.db"
                
                # Check if processed file already exists in S3
                if self._check_s3_file_exists(processed_s3_key):
                    logger.info(f"Processed file already exists in S3: {processed_s3_key}")
                    return True
                
                # Download raw file from S3
                local_raw_file = os.path.join(self.local_temp_dir, f"{year}-{month:02d}_raw.txt")
                if not self._download_from_s3(raw_s3_key, local_raw_file):
                    logger.error(f"Failed to download raw file from S3: {raw_s3_key}")
                    return False
                
                # Process the file
                local_processed_file = os.path.join(self.local_temp_dir, f"{year}-{month:02d}_processed.db")
                success = self._process_file(local_raw_file, local_processed_file, year, month)
                
                if success:
                    # Upload processed file to S3
                    self._upload_to_s3(local_processed_file, processed_s3_key)
                    
                    # Cleanup local files
                    self._cleanup_local_files([local_raw_file, local_processed_file])
                    
                    logger.info(f"Successfully processed and uploaded {processed_s3_key}")
                    return True
                else:
                    logger.error("Processing failed")
                    self._cleanup_local_files([local_raw_file, local_processed_file])
                    return False
                
        except Exception as e:
            logger.error(f"Error processing player data: {str(e)}")
            raise
    
    def _process_file(self, input_filename, output_filename, year, month, time_control=None):
        """Process the raw FIDE data file and convert to SQLite format"""
        try:
            lengths, keys = self._get_format_config(year, month)
            
            # Create SQLite database
            conn = sqlite3.connect(output_filename)
            conn.execute('PRAGMA journal_mode=WAL')  # Better performance for writes
            conn.execute('PRAGMA synchronous=NORMAL')  # Better performance
            
            # Create table with appropriate schema based on format
            create_table_sql = self._get_create_table_sql(keys)
            conn.execute(create_table_sql)
            
            # Create indexes for fast lookups
            # Primary key (id) is automatically indexed
            conn.execute('CREATE INDEX idx_name ON players(name)')
            
            # Prepare insert statement with OR IGNORE to handle duplicates
            placeholders = ', '.join(['?' for _ in keys])
            insert_sql = f"INSERT OR IGNORE INTO players ({', '.join(keys)}) VALUES ({placeholders})"
            
            # Keep track of seen IDs for duplicate detection
            seen_ids = {}
            duplicate_count = 0
            
            with open(input_filename, "r", encoding="utf-8", errors="replace") as input_file:
                
                # Skip header line only for certain dates
                if not (
                    (year == 2003 and month in [7, 10])
                    or (year == 2004 and month == 1)
                    or (year == 2005 and month == 4)
                ):
                    input_file.readline()
                
                line_count = 0
                processed_count = 0
                
                for line in input_file:
                    try:
                        parts = self._process_line(line, lengths)
                        # Convert data types as needed
                        processed_parts = self._convert_data_types(parts, keys)
                        
                        # Get the ID (first element)
                        player_id = processed_parts[0]
                        
                        # Check for duplicates
                        if player_id in seen_ids:
                            duplicate_count += 1
                            # Log warning with both sets of attributes
                            original_attrs = dict(zip(keys, seen_ids[player_id]))
                            current_attrs = dict(zip(keys, processed_parts))
                            
                            logger.warning(f"Duplicate ID found: {player_id}")
                            logger.warning(f"  Original record: {original_attrs}")
                            logger.warning(f"  Duplicate record: {current_attrs}")
                            logger.warning(f"  Ignoring duplicate record")
                        else:
                            # Store the first occurrence
                            seen_ids[player_id] = processed_parts
                        
                        # Execute insert (will be ignored if duplicate due to OR IGNORE)
                        cursor = conn.execute(insert_sql, processed_parts)
                        
                        # Check if the row was actually inserted
                        if cursor.rowcount > 0:
                            processed_count += 1
                        
                        line_count += 1
                        
                        # Log progress for large files
                        if line_count % 10000 == 0:
                            logger.info(f"Processed {line_count} lines, {processed_count} records inserted, {duplicate_count} duplicates ignored")
                            
                    except Exception as e:
                        logger.warning(f"Failed to process line {line_count + 1}: {str(e)}")
                        continue
                
                conn.commit()
                conn.close()
                
                logger.info(f"Processing complete for {time_control if time_control else 'standard'} rating list:")
                logger.info(f"  Total lines processed: {line_count}")
                logger.info(f"  Records inserted: {processed_count}")
                logger.info(f"  Duplicates ignored: {duplicate_count}")
                
                return True
                
        except Exception as e:
            logger.error(f"Error processing file {input_filename}: {str(e)}")
            return False
    
    def _get_create_table_sql(self, keys):
        """Generate CREATE TABLE SQL based on the keys"""
        # Define column types based on field names
        type_mapping = {
            'id': 'TEXT PRIMARY KEY',
            'name': 'TEXT NOT NULL',
            'title': 'TEXT',
            'fed': 'TEXT',
            'rating': 'INTEGER',
            'games': 'INTEGER',
            'b_year': 'INTEGER',
            'sex': 'TEXT',
            'flag': 'TEXT',
            'w_title': 'TEXT',
            'o_title': 'TEXT',
            'foa': 'TEXT',
            'k': 'INTEGER'
        }
        
        columns = []
        for key in keys:
            column_type = type_mapping.get(key, 'TEXT')
            columns.append(f"{key} {column_type}")
        
        return f"CREATE TABLE players ({', '.join(columns)})"
    
    def _convert_data_types(self, parts, keys):
        """Convert string data to appropriate types"""
        # Determine if this is a format where sex needs to be extracted from flag
        needs_sex_extraction = "sex" in keys and len(parts) == len(keys) - 1
        
        converted = []
        parts_index = 0
        
        for i, key in enumerate(keys):
            if key == "sex" and needs_sex_extraction:
                # Extract sex from flag field - flag should be the previous field
                flag_index = keys.index("flag") if "flag" in keys else -1
                if flag_index >= 0 and flag_index < len(parts):
                    flag_value = parts[flag_index] if parts[flag_index] else ""
                    # Player is female (F) if flag contains 'w', otherwise male (M)
                    sex_value = "F" if "w" in flag_value.lower() else "M"
                    converted.append(sex_value)
                else:
                    converted.append("M")  # Default to male if flag not found
                # Don't increment parts_index since we're extracting from existing data
            else:
                # Normal field processing
                if parts_index < len(parts):
                    part = parts[parts_index]
                    if key in ['rating', 'games', 'b_year', 'k']:
                        # Convert to integer, handle empty strings
                        try:
                            converted.append(int(part) if part else None)
                        except ValueError:
                            converted.append(None)
                    else:
                        # Keep as string, but convert empty strings to None for cleaner data
                        converted.append(part if part else None)
                    parts_index += 1
                else:
                    # Handle missing data
                    if key in ['rating', 'games', 'b_year', 'k']:
                        converted.append(None)
                    else:
                        converted.append(None)
        
        return converted
    
    def _process_line(self, line, lengths):
        """Splits a line into parts of fixed lengths."""
        parts = []
        start = 0
        for length in lengths:
            part = line[start : start + length].strip()
            parts.append(part)
            start += length
        return parts
    
    def _get_format_config(self, year, month):
        """
        Determine the format configuration based on the date.
        Returns a tuple of (lengths, keys) for the given year and month.
        """
        if year == 2001:
            if month <= 4:
                return (
                    [10, 33, 6, 8, 6, 6, 11, 4],
                    ["id", "name", "title", "fed", "rating", "games", "b_year", "flag", "sex"],
                )
            else:
                return (
                    [10, 33, 6, 8, 6, 6, 11, 4, 8],
                    [
                        "id",
                        "name",
                        "title",
                        "fed",
                        "rating",
                        "games",
                        "b_year",
                        "sex",
                        "flag",
                    ],
                )

        if year == 2002 and month < 10:
            if month == 4:
                return (
                    [10, 33, 6, 8, 6, 6, 11, 4, 6],
                    [
                        "id",
                        "name",
                        "title",
                        "fed",
                        "rating",
                        "games",
                        "b_year",
                        "sex",
                        "flag",
                    ],
                )
            return (
                [10, 33, 6, 8, 6, 6, 11, 6],
                ["id", "name", "title", "fed", "rating", "games", "b_year", "flag", "sex"],
            )
        if year < 2005 or (year == 2005 and month <= 7):
            return (
                [9, 32, 6, 8, 6, 5, 11, 4],
                ["id", "name", "title", "fed", "rating", "games", "b_year", "flag", "sex"],
            )
        if year < 2012 or (year == 2012 and month <= 8):
            return (
                [10, 32, 6, 4, 6, 4, 6, 4],
                ["id", "name", "title", "fed", "rating", "games", "b_year", "flag", "sex"],
            )
        if year < 2016 or (year == 2016 and month <= 8):
            return (
                [15, 61, 4, 4, 5, 5, 15, 6, 4, 3, 6, 4],
                [
                    "id",
                    "name",
                    "fed",
                    "sex",
                    "title",
                    "w_title",
                    "o_title",
                    "rating",
                    "games",
                    "k",
                    "b_year",
                    "flag",
                ],
            )

        return (
            [15, 61, 4, 4, 5, 5, 15, 4, 6, 4, 3, 6, 4],
            [
                "id",
                "name",
                "fed",
                "sex",
                "title",
                "w_title",
                "o_title",
                "foa",
                "rating",
                "games",
                "k",
                "b_year",
                "flag",
            ],
        )
    
    def _check_s3_file_exists(self, s3_key):
        """Check if file exists in S3"""
        try:
            self.s3_client.head_object(Bucket=self.s3_bucket, Key=s3_key)
            return True
        except ClientError:
            return False
    
    def _download_from_s3(self, s3_key, local_path):
        """Download file from S3"""
        try:
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            self.s3_client.download_file(self.s3_bucket, s3_key, local_path)
            logger.info(f"Downloaded {s3_key} from S3 to {local_path}")
            return True
        except ClientError as e:
            logger.error(f"Failed to download from S3: {str(e)}")
            return False
    
    def _upload_to_s3(self, local_path, s3_key):
        """Upload file to S3"""
        try:
            self.s3_client.upload_file(local_path, self.s3_bucket, s3_key)
            logger.info(f"Uploaded {local_path} to S3 as {s3_key}")
        except ClientError as e:
            logger.error(f"Failed to upload to S3: {str(e)}")
            raise  # This is critical for the pipeline
    
    def _cleanup_local_files(self, file_paths):
        """Clean up local temporary files"""
        for file_path in file_paths:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    logger.debug(f"Cleaned up {file_path}")
            except Exception as e:
                logger.warning(f"Failed to cleanup {file_path}: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Process FIDE player information.")
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
    processor = FIDEDataProcessor(
        s3_bucket=args.s3_bucket,
        aws_region=args.aws_region
    )
    
    # Process data
    success = processor.process_month_data(args.month)
    
    if success:
        logger.info(f"Player data processing completed successfully for {args.month}")
        return 0
    else:
        logger.error("Player data processing failed")
        return 1

if __name__ == "__main__":
    exit(main())