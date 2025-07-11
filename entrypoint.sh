#!/bin/bash
set -e

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Default configuration
S3_BUCKET=${S3_BUCKET:-}
AWS_REGION=${AWS_REGION:-us-east-2}
PIPELINE_MODE=${PIPELINE_MODE:-full}

# Get process month from environment or calculate it
if [ -z "$PROCESS_MONTH" ]; then
    # Default to previous month
    PROCESS_MONTH=$(date -d "$(date +'%Y-%m-01') -1 month" +'%Y-%m')
fi

log "Starting Chess Rating Pipeline"
log "Pipeline Mode: $PIPELINE_MODE"
log "Process Month: $PROCESS_MONTH"
log "S3 Bucket: $S3_BUCKET"

# Validate required environment variables
if [ -z "$S3_BUCKET" ]; then
    log "ERROR: S3_BUCKET environment variable is required"
    exit 1
fi

# Function to validate data exists for post-scraping mode
validate_post_scraping_data() {
    log "Validating required data for post-scraping mode..."
    
    local missing_data=""
    
    # Check if processed player data exists
    if ! aws s3 ls "s3://$S3_BUCKET/persistent/player_info/processed/standard/$PROCESS_MONTH.db" >/dev/null 2>&1; then
        missing_data="$missing_data\n- Player info processed data (persistent/player_info/processed/standard/$PROCESS_MONTH.db)"
    fi
    
    # Check if active players data exists for at least standard time control
    if ! aws s3 ls "s3://$S3_BUCKET/persistent/active_players/${PROCESS_MONTH}_standard.txt" >/dev/null 2>&1; then
        missing_data="$missing_data\n- Active players data (persistent/active_players/${PROCESS_MONTH}_standard.txt)"
    fi
    
    # Check if calculation data exists
    local calc_count=$(aws s3 ls "s3://$S3_BUCKET/persistent/calculations/$PROCESS_MONTH/" --recursive | wc -l)
    if [ "$calc_count" -eq 0 ]; then
        missing_data="$missing_data\n- Calculation data (persistent/calculations/$PROCESS_MONTH/)"
    fi
    
    # Check if processed calculation data already exists (to avoid reprocessing)
    local processed_calc_count=$(aws s3 ls "s3://$S3_BUCKET/persistent/calculations_processed/" --recursive | grep "$PROCESS_MONTH" | wc -l)
    if [ "$processed_calc_count" -gt 0 ]; then
        log "⚠️  Processed calculation data already exists for $PROCESS_MONTH"
        log "Found $processed_calc_count processed calculation files"
        log "The calculation processor will skip processing if data already exists"
    fi
    
    if [ -n "$missing_data" ]; then
        log "ERROR: Required data is missing for post-scraping mode:"
        echo -e "$missing_data"
        log ""
        log "The post-scraping mode requires that the following pipeline steps have been completed:"
        log "1. Download player data"
        log "2. Process player data"
        log "3. Scrape and process tournament data"
        log "4. Aggregate player IDs"
        log "5. Scrape player calculations"
        log ""
        log "Please run the full pipeline first, or use PIPELINE_MODE=full to run all steps."
        exit 1
    else
        log "✅ All required data found for post-scraping mode"
        log "Found calculation files: $calc_count"
    fi
}

# Function to run full pipeline
run_full_pipeline() {
    log "Running full pipeline (all steps)..."
    
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

    # Step 5: Scrape Player Calculations (Rust)
    log "Step 5: Scraping player calculation data with Rust..."
    calculation-scraper \
        --month "$PROCESS_MONTH" \
        --s3-bucket "$S3_BUCKET" \
        --aws-region "$AWS_REGION"

    if [ $? -ne 0 ]; then
        log "ERROR: Player calculation scraping failed"
        exit 1
    fi

    log "Step 5 completed successfully"
    
    # Continue to post-scraping steps
    run_post_scraping_pipeline
}

# Function to run post-scraping pipeline
run_post_scraping_pipeline() {
    log "Running post-scraping pipeline steps..."
    
    # Step 6: Process Calculation Data (Rust)
    log "Step 6: Processing calculation data with Rust..."
    calculation-processor \
        --month "$PROCESS_MONTH" \
        --s3-bucket "$S3_BUCKET" \
        --aws-region "$AWS_REGION"

    if [ $? -ne 0 ]; then
        log "ERROR: Calculation data processing failed"
        exit 1
    fi

    log "Step 6 completed successfully"

    run_glicko_only_pipeline
}

# Function to run glicko-only pipeline
run_glicko_only_pipeline() {
    log "Running Glicko steps..."
    
    # Step 7: Calculate Ratings
    log "Step 7: Calculating ratings with Glicko-2 algorithm..."
    
    # Add debugging
    log "Checking if run-glicko is available..."
    which run-glicko || log "run-glicko not found in PATH"
    ls -la /app/bin/run-glicko || log "run-glicko binary not found at /app/bin/run-glicko"
    
    log "Current PATH: $PATH"
    log "Running run-glicko with parameters:"
    log "  --month $PROCESS_MONTH"
    log "  --s3-bucket $S3_BUCKET"
    log "  --aws-region $AWS_REGION"
    
    # Test if binary can execute at all
    log "Testing binary execution..."
    if ! /app/bin/run-glicko --help >/dev/null 2>&1; then
        log "ERROR: run-glicko binary cannot execute - checking dependencies..."
        ldd /app/bin/run-glicko || log "ldd failed"
        file /app/bin/run-glicko || log "file failed"
        exit 1
    fi
    
    log "Binary test passed, running with full parameters..."
    
    # Set multiple Rust logging levels
    export RUST_LOG=debug
    export RUST_BACKTRACE=1
    
    log "Environment variables set:"
    log "  RUST_LOG=$RUST_LOG"
    log "  RUST_BACKTRACE=$RUST_BACKTRACE"
    
    # Try a more verbose approach - run directly with strace to see what's happening
    log "Attempting to trace system calls..."
    
    # First try with timeout to prevent hanging
    log "Running with 30 second timeout..."
    set +e  # Don't exit on error
    if timeout 30 strace -o /tmp/strace.log -e trace=write,openat,execve run-glicko \
        --month "$PROCESS_MONTH" \
        --s3-bucket "$S3_BUCKET" \
        --aws-region "$AWS_REGION" > /tmp/run-glicko.out 2>&1; then
        exit_code=0
    else
        exit_code=$?
    fi
    set -e  # Re-enable exit on error
    
    log "run-glicko exit code: $exit_code"
    
    # Show output files
    if [ -f /tmp/run-glicko.out ]; then
        output_size=$(wc -c < /tmp/run-glicko.out)
        log "Output file size: $output_size bytes"
        if [ $output_size -gt 0 ]; then
            log "run-glicko output:"
            cat /tmp/run-glicko.out
        else
            log "Output file is empty"
        fi
    fi
    
    if [ -f /tmp/strace.log ]; then
        log "System call trace (last 20 lines):"
        tail -20 /tmp/strace.log
    fi
    
    # Cleanup
    rm -f /tmp/run-glicko.out /tmp/strace.log

    if [ $exit_code -ne 0 ]; then
        log "ERROR: Rating calculation failed with exit code $exit_code"
        exit 1
    fi

    log "Step 7 completed successfully"

    # Step 8: Upload Results (Future implementation)
    log "Step 8: Uploading results..."
    log "⚠️  Results upload step not yet implemented"
    log "This step will:"
    log "- Upload calculated ratings"
    log "- Generate summary reports"
    log "- Create performance metrics"
    log "- Send notifications"
    
    # Placeholder for results upload
    # python src/upload_results.py \
    #     --month "$PROCESS_MONTH" \
    #     --s3_bucket "$S3_BUCKET" \
    #     --aws_region "$AWS_REGION"
    
    log "Step 8 placeholder completed"
}

# Function to upload completion marker
upload_completion_marker() {
    local mode=$1
    local status=$2
    
    if [ -n "$S3_BUCKET" ]; then
        log "Uploading completion marker to S3..."
        
        # Create a completion marker with details
        cat > /tmp/completion.txt << EOF
Pipeline completed at $(date -u +%Y-%m-%dT%H:%M:%SZ) for month $PROCESS_MONTH
Mode: $mode
Status: $status

Steps completed based on mode:
EOF

        if [ "$mode" = "full" ]; then
            cat >> /tmp/completion.txt << EOF
1. Downloaded player data from FIDE
2. Processed player data to JSON format
3. Scraped tournament IDs and processed tournament data in parallel
4. Aggregated unique player IDs by time control
5. Scraped individual player calculation data (games and results)
6. Processed calculation data into compact binary format (Rust)
7. Calculated ratings using Glicko-2 algorithm (Rust)
8. Uploaded results (placeholder)
EOF
        elif [ "$mode" = "post_scraping" ]; then
            cat >> /tmp/completion.txt << EOF
6. Processed calculation data into compact binary format (Rust)
7. Calculated ratings using Glicko-2 algorithm (Rust)
8. Uploaded results (placeholder)

Note: Steps 1-5 were skipped (post-scraping mode)
EOF
        else
            cat >> /tmp/completion.txt << EOF
7. Calculated ratings using Glicko-2 algorithm (Rust)
8. Uploaded results (placeholder)

Note: Steps 1-6 were skipped (glicko-only mode)
EOF
        fi
        
        cat >> /tmp/completion.txt << EOF

Files created/modified:
EOF

        if [ "$mode" = "full" ]; then
            cat >> /tmp/completion.txt << EOF
- s3://$S3_BUCKET/persistent/player_info/raw/$PROCESS_MONTH.txt
- s3://$S3_BUCKET/persistent/player_info/processed/$PROCESS_MONTH.txt
- s3://$S3_BUCKET/persistent/tournament_data/processed/[time_control]/$PROCESS_MONTH/[tournament_id].txt
- s3://$S3_BUCKET/persistent/active_players/$PROCESS_MONTH_[time_control].txt
- s3://$S3_BUCKET/persistent/calculations/$PROCESS_MONTH/[time_control]/[player_id].json
EOF
        fi
        
        cat >> /tmp/completion.txt << EOF
- s3://$S3_BUCKET/persistent/calculations_processed/$PROCESS_MONTH/[time_control].bin.gz
- s3://$S3_BUCKET/results/$PROCESS_MONTH/calculation_processing_completion.json
- s3://$S3_BUCKET/persistent/ratings/$PROCESS_MONTH/[time_control].parquet
- s3://$S3_BUCKET/persistent/top_ratings/$PROCESS_MONTH/[time_control]/[category].json
- s3://$S3_BUCKET/results/$PROCESS_MONTH/rating_processing_completion.json

Data structure:
- Tournament data is organized by time control (standard/rapid/blitz)
- Each tournament file contains JSON lines with player ID and name
- Active player lists contain unique player IDs per time control for the month
- Calculation files contain detailed game data and results for each player
- Processed calculations are in compact binary format optimized for rating calculations
- Player data is processed and ready for rating calculations
- Ratings are stored in Parquet format with rating, RD, and volatility values for each player
- Top rating lists contain ranked lists by category (open, women, juniors, girls) with player details
- All rating data uses Glicko-2 algorithm with proper uncertainty and volatility tracking
EOF
        
        aws s3 cp /tmp/completion.txt "s3://$S3_BUCKET/results/$PROCESS_MONTH/completion_${mode}.txt"
        rm /tmp/completion.txt
    fi
}

# Main execution logic
case "$PIPELINE_MODE" in
    "full")
        log "Starting full pipeline mode..."
        run_full_pipeline
        upload_completion_marker "full" "success"
        log "Full Chess Rating Pipeline completed successfully for $PROCESS_MONTH"
        ;;
    "post_scraping")
        log "Starting post-scraping mode..."
        validate_post_scraping_data
        run_post_scraping_pipeline
        upload_completion_marker "post_scraping" "success"
        log "Post-scraping Chess Rating Pipeline completed successfully for $PROCESS_MONTH"
        ;;
    "glicko_only")
        log "Starting Glicko-only pipeline mode..."
        validate_post_scraping_data
        run_glicko_only_pipeline
        upload_completion_marker "glicko_only" "success"
        log "Glicko-only Chess Rating Pipeline completed successfully for $PROCESS_MONTH"
        ;;
    *)
        log "ERROR: Invalid PIPELINE_MODE: $PIPELINE_MODE"
        log "Valid modes: full, post_scraping, glicko_only"
        exit 1
        ;;
esac

# Optional: Send notification (if SNS topic is configured)
if [ -n "$SNS_TOPIC_ARN" ]; then
    message="Chess Rating Pipeline ($PIPELINE_MODE mode) completed successfully for $PROCESS_MONTH."
    
    if [ "$PIPELINE_MODE" = "full" ]; then
        message="$message Downloaded player data, processed it, scraped/processed tournament data in parallel, aggregated unique player IDs by time control, scraped individual player calculation data, processed calculations into binary format, and calculated ratings using Glicko-2 algorithm."
    elif [ "$PIPELINE_MODE" = "post_scraping" ]; then
        message="$message Processed calculation data into binary format and calculated ratings using Glicko-2 algorithm."
    else
        message="$message Calculated ratings using Glicko-2 algorithm only."
    fi
    
    aws sns publish \
        --topic-arn "$SNS_TOPIC_ARN" \
        --message "$message" \
        --subject "Chess Pipeline Success ($PIPELINE_MODE) - $PROCESS_MONTH" \
        --region "$AWS_REGION" || log "Warning: Failed to send SNS notification"
fi