import os
import requests
import zipfile
import argparse
import boto3
import logging
from datetime import datetime
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DEFAULT_AWS_REGION = "us-east-2"

class PlayerDataDownloader:
    def __init__(self, s3_bucket, aws_region=DEFAULT_AWS_REGION):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3', region_name=aws_region)
        self.base_url = "https://ratings.fide.com/download/"
        self.local_temp_dir = "/tmp/chess_data"
        self.month_mappings = {
            1: "jan", 2: "feb", 3: "mar", 4: "apr", 5: "may", 6: "jun",
            7: "jul", 8: "aug", 9: "sep", 10: "oct", 11: "nov", 12: "dec"
        }
        
    def download_player_data(self, month_str):
        """Download FIDE player data for a specific month"""
        try:
            year, month = map(int, month_str.split("-"))
            logger.info(f"Starting download for {year}-{month:02d}")
            
            # Setup local temp directory
            os.makedirs(self.local_temp_dir, exist_ok=True)
            
            # Determine if we need multiple rating lists (after 2012)
            needs_multiple_lists = year > 2012 or (year == 2012 and month >= 9)
            
            if needs_multiple_lists:
                # Download all three rating lists
                downloaded_files = []
                time_controls = ["standard", "rapid", "blitz"]
                
                for time_control in time_controls:
                    local_file = os.path.join(self.local_temp_dir, f"{year}-{month:02d}.txt")
                    s3_key = f"persistent/player_info/raw/{time_control}/{year}-{month:02d}.txt"
                    
                    # Check if file already exists in S3
                    if self._check_s3_file_exists(s3_key):
                        logger.info(f"{time_control} file exists in S3.")
                        continue
                    else:
                        # Download from FIDE website
                        success = self._download_from_fide(year, month, local_file, time_control)
                        
                        if success:
                            # Upload to S3 for persistence
                            self._upload_to_s3(local_file, s3_key)
                            downloaded_files.append(s3_key)
                        else:
                            logger.warning(f"Failed to download {time_control} rating list")
                
                return downloaded_files if downloaded_files else None
            else:
                # Download only standard rating list (pre-2012 format)
                local_file = os.path.join(self.local_temp_dir, f"{year}-{month:02d}.txt")
                s3_key = f"persistent/player_info/raw/{year}-{month:02d}.txt"
                
                # Check if file already exists in S3
                if self._check_s3_file_exists(s3_key):
                    logger.info(f"File exists in S3.")
                    return None
                
                # Download from FIDE website
                success = self._download_from_fide(year, month, local_file)
                
                if success:
                    # Upload to S3 for persistence
                    self._upload_to_s3(local_file, s3_key)
                    return [s3_key]
                else:
                    return None
                
        except Exception as e:
            logger.error(f"Error downloading player data: {str(e)}")
            raise
    
    def _download_from_fide(self, year, month, local_file, time_control=None):
        """Download player data from FIDE website"""
        month_str = self.month_mappings[month]
        year_str = str(year)[2:]
        
        # Determine ZIP file naming convention
        if time_control == "standard":
            zip_header = f"standard_{month_str}{year_str}"
        elif time_control == "rapid":
            zip_header = f"rapid_{month_str}{year_str}"
        elif time_control == "blitz":
            zip_header = f"blitz_{month_str}{year_str}"
        else:
            # Pre-2012 format (no time control prefix)
            zip_header = f"{month_str}{year_str}"
        
        url = f"{self.base_url}{zip_header}frl.zip"
        zip_path = os.path.join(self.local_temp_dir, f"{zip_header}frl.zip")
        
        try:
            if time_control:
                logger.info(f"Downloading {time_control} rating list from {url}")
            else:
                logger.info(f"Downloading rating list from {url}")
            response = requests.get(url, timeout=300)  # 5 minute timeout
            response.raise_for_status()
            
            # Save ZIP file
            with open(zip_path, "wb") as file:
                file.write(response.content)
            
            # Extract and rename
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(self.local_temp_dir)
                extracted_file = os.path.join(self.local_temp_dir, f"{zip_header}frl.txt")
                if os.path.exists(extracted_file):
                    os.rename(extracted_file, local_file)
                else:
                    # Sometimes the file structure is different
                    for file in zip_ref.namelist():
                        if file.endswith('.txt'):
                            os.rename(os.path.join(self.local_temp_dir, file), local_file)
                            break
            
            # Cleanup
            os.remove(zip_path)
            
            if time_control:
                logger.info(f"Successfully downloaded and extracted {time_control} rating list: {local_file}")
            else:
                logger.info(f"Successfully downloaded and extracted rating list: {local_file}")
            return True
            
        except requests.exceptions.RequestException as e:
            if time_control:
                logger.error(f"Failed to download {time_control} rating list from {url}: {str(e)}")
            else:
                logger.error(f"Failed to download rating list from {url}: {str(e)}")
            return False
        except zipfile.BadZipFile as e:
            logger.error(f"Failed to extract {zip_path}: {str(e)}")
            if os.path.exists(zip_path):
                os.remove(zip_path)
            return False
        except Exception as e:
            if time_control:
                logger.error(f"Unexpected error during {time_control} FIDE download: {str(e)}")
            else:
                logger.error(f"Unexpected error during FIDE download: {str(e)}")
            return False
    
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
            logger.info(f"Downloaded {s3_key} from S3")
        except ClientError as e:
            logger.error(f"Failed to download from S3: {str(e)}")
            raise
    
    def _upload_to_s3(self, local_path, s3_key):
        """Upload file to S3"""
        try:
            self.s3_client.upload_file(local_path, self.s3_bucket, s3_key)
            logger.info(f"Uploaded {local_path} to S3 as {s3_key}")
        except ClientError as e:
            logger.error(f"Failed to upload to S3: {str(e)}")
            raise Exception(f"Failed to upload to S3: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="Download FIDE player information.")
    parser.add_argument(
        "--month",
        type=str,
        help="Month for the download in YYYY-MM format",
        required=True,
    )
    parser.add_argument(
        "--s3_bucket",
        type=str,
        help="S3 bucket for persistent storage",
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
    
    # Initialize downloader
    downloader = PlayerDataDownloader(
        s3_bucket=args.s3_bucket,
        aws_region=args.aws_region
    )
    
    # Download data
    result = downloader.download_player_data(args.month)
    
    if result:
        if isinstance(result, list):
            logger.info(f"Player data download completed successfully. Downloaded {len(result)} files:")
            for file_path in result:
                logger.info(f"  - {file_path}")
        else:
            logger.info(f"Player data download completed successfully: {result}")
        return 0
    else:
        logger.error("Player data download failed")
        return 1

if __name__ == "__main__":
    exit(main())