#!/bin/bash
set -e

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Default configuration
S3_BUCKET=${S3_BUCKET:-}
AWS_REGION=${AWS_REGION:-us-east-2}

# Get process month from environment or calculate it
if [ -z "$PROCESS_MONTH" ]; then
    # Default to previous month
    PROCESS_MONTH=$(date -d "$(date +'%Y-%m-01') -1 month" +'%Y-%m')
fi

log "Starting Chess Rating Pipeline"
log "Process Month: $PROCESS_MONTH"
log "S3 Bucket: $S3_BUCKET"

# Validate required environment variables
if [ -z "$S3_BUCKET" ]; then
    log "ERROR: S3_BUCKET environment variable is required"
    exit 1
fi

# Step 1: Download Player Data
log "Step 1: Downloading player data..."
python src/download_player_data.py \
    --month "$PROCESS_MONTH" \
    --s3_bucket "$S3_BUCKET" \
    --aws_region "$AWS_REGION"

if [ $? -ne 0 ]; then
    log "ERROR: Player data download failed"
    exit 1
fi

log "Step 1 completed successfully"

# Step 2: Process Player Data
log "Step 2: Processing player data..."
python src/process_fide_rating_list.py \
    --month "$PROCESS_MONTH" \
    --s3_bucket "$S3_BUCKET" \
    --aws_region "$AWS_REGION"

if [ $? -ne 0 ]; then
    log "ERROR: Player data processing failed"
    exit 1
fi

log "Step 2 completed successfully"

# Step 3: Scrape and Process Tournament Data (Combined)
log "Step 3: Scraping and processing tournament data..."
python src/tournament_scraper.py \
    --month "$PROCESS_MONTH" \
    --s3_bucket "$S3_BUCKET" \
    --aws_region "$AWS_REGION"

if [ $? -ne 0 ]; then
    log "ERROR: Tournament data scraping and processing failed"
    exit 1
fi

log "Step 3 completed successfully"

# Step 4: Aggregate Player IDs
log "Step 4: Aggregating player IDs by time control..."
python src/aggregate_player_ids.py \
    --month "$PROCESS_MONTH" \
    --s3_bucket "$S3_BUCKET" \
    --aws_region "$AWS_REGION"

if [ $? -ne 0 ]; then
    log "ERROR: Player ID aggregation failed"
    exit 1
fi

log "Step 4 completed successfully"

# Step 5: Scrape Player Calculations
log "Step 5: Scraping player calculation data..."
python src/scrape_calculations.py \
    --month "$PROCESS_MONTH" \
    --s3_bucket "$S3_BUCKET" \
    --aws_region "$AWS_REGION"

if [ $? -ne 0 ]; then
    log "ERROR: Player calculation scraping failed"
    exit 1
fi

log "Step 5 completed successfully"

# Future steps would go here:
# Step 6: Calculate ratings
# Step 7: Upload results

# Upload the completion marker to show all completed steps
if [ -n "$S3_BUCKET" ]; then
    log "Uploading results to S3..."
    
    # Create a completion marker with more details
    cat > /tmp/completion.txt << EOF
Pipeline completed at $(date -u +%Y-%m-%dT%H:%M:%SZ) for month $PROCESS_MONTH

Steps completed:
1. Downloaded player data from FIDE
2. Processed player data to JSON format
3. Scraped tournament IDs and processed tournament data in parallel
4. Aggregated unique player IDs by time control
5. Scraped individual player calculation data (games and results)

Files created:
- s3://$S3_BUCKET/persistent/player_info/raw/$PROCESS_MONTH.txt
- s3://$S3_BUCKET/persistent/player_info/processed/$PROCESS_MONTH.txt
- s3://$S3_BUCKET/persistent/tournament_data/processed/[time_control]/$PROCESS_MONTH/[tournament_id].txt
- s3://$S3_BUCKET/persistent/active_players/$PROCESS_MONTH_[time_control].txt
- s3://$S3_BUCKET/persistent/calculations/$PROCESS_MONTH/[time_control]/[player_id].json

Data structure:
- Tournament data is organized by time control (standard/rapid/blitz)
- Each tournament file contains JSON lines with player ID and name
- Active player lists contain unique player IDs per time control for the month
- Calculation files contain detailed game data and results for each player
- Player data is processed and ready for rating calculations
- No intermediate tournament ID files stored (processed directly)
EOF
    
    aws s3 cp /tmp/completion.txt "s3://$S3_BUCKET/results/$PROCESS_MONTH/completion.txt"
fi

log "Chess Rating Pipeline completed successfully for $PROCESS_MONTH"

# Optional: Send notification (if SNS topic is configured)
if [ -n "$SNS_TOPIC_ARN" ]; then
    aws sns publish \
        --topic-arn "$SNS_TOPIC_ARN" \
        --message "Chess Rating Pipeline completed successfully for $PROCESS_MONTH. Downloaded player data, processed it, scraped/processed tournament data in parallel, aggregated unique player IDs by time control, and scraped individual player calculation data." \
        --subject "Chess Pipeline Success - $PROCESS_MONTH" \
        --region "$AWS_REGION" || log "Warning: Failed to send SNS notification"
fi