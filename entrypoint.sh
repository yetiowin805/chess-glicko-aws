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

# Future steps would go here:
# Step 2: Download tournament data
# Step 3: Download game data  
# Step 4: Process data
# Step 5: Calculate ratings
# Step 6: Upload results

# For now, let's upload the player data to show completion
if [ -n "$S3_BUCKET" ]; then
    log "Uploading results to S3..."
    
    # Create a completion marker
    echo "Pipeline completed at $(date -u +%Y-%m-%dT%H:%M:%SZ) for month $PROCESS_MONTH" > /tmp/completion.txt
    aws s3 cp /tmp/completion.txt "s3://$S3_BUCKET/results/$PROCESS_MONTH/completion.txt"
fi

log "Chess Rating Pipeline completed successfully for $PROCESS_MONTH"

# Optional: Send notification (if SNS topic is configured)
if [ -n "$SNS_TOPIC_ARN" ]; then
    aws sns publish \
        --topic-arn "$SNS_TOPIC_ARN" \
        --message "Chess Rating Pipeline completed successfully for $PROCESS_MONTH" \
        --subject "Chess Pipeline Success - $PROCESS_MONTH" \
        --region "$AWS_REGION" || log "Warning: Failed to send SNS notification"
fi