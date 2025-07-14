#!/bin/bash
set -eEux
trap 'echo "Script failed at line $LINENO with exit code $?"; exit 1' ERR

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Check if we're being called with run-glicko arguments (for glicko_only mode with month ranges)
if [ $# -gt 0 ] && ([ "$1" = "run-glicko" ] || [ "$1" = "/app/bin/run-glicko" ]); then
    log "Detected run-glicko command, executing directly..."
    exec "$@"
fi

# Function to check if a month is a valid FIDE rating period
is_valid_rating_period() {
    local year=$1
    local month=$2
    
    if [ "$year" -lt 2009 ]; then
        # Quarterly periods: Jan, Apr, Jul, Oct
        case "$month" in
            1|4|7|10) return 0 ;;
            *) return 1 ;;
        esac
    elif [ "$year" -eq 2009 ]; then
        if [ "$month" -lt 9 ]; then
            # First half of 2009: Jan, Apr, Jul
            case "$month" in
                1|4|7) return 0 ;;
                *) return 1 ;;
            esac
        else
            # Second half of 2009: odd months (Sep, Nov)
            if [ $((month % 2)) -eq 1 ]; then
                return 0
            else
                return 1
            fi
        fi
    elif [ "$year" -lt 2012 ]; then
        # Odd months only
        if [ $((month % 2)) -eq 1 ]; then
            return 0
        else
            return 1
        fi
    elif [ "$year" -eq 2012 ]; then
        if [ "$month" -lt 8 ]; then
            # First half of 2012: odd months
            if [ $((month % 2)) -eq 1 ]; then
                return 0
            else
                return 1
            fi
        else
            # From August 2012 onwards: all months
            return 0
        fi
    else
        # After 2012: all months
        return 0
    fi
}

# Default configuration
S3_BUCKET=${S3_BUCKET:-}
AWS_REGION=${AWS_REGION:-us-east-2}
PIPELINE_MODE=${PIPELINE_MODE:-full}

# Get process month from environment or calculate it
if [ -z "$PROCESS_MONTH" ]; then
    if [ "$PIPELINE_MODE" = "glicko_only" ]; then
        # For glicko_only mode, we don't need a specific month as it will be handled by the run-glicko command
        # Set a default value to avoid the unbound variable error
        PROCESS_MONTH="default"
        log "PROCESS_MONTH not set for glicko_only mode, using default value"
    else
        # Default to previous month for other modes
        PROCESS_MONTH=$(date -d "$(date +'%Y-%m-01') -1 month" +'%Y-%m')
    fi
fi

log "Starting Chess Rating Pipeline"
log "Pipeline Mode: $PIPELINE_MODE"
log "Process Month: $PROCESS_MONTH"
log "S3 Bucket: $S3_BUCKET"

# Parse year and month from PROCESS_MONTH
if [ "$PROCESS_MONTH" = "default" ] && [ "$PIPELINE_MODE" = "glicko_only" ]; then
    # For glicko_only mode with default PROCESS_MONTH, skip FIDE period validation
    # as the actual months will be handled by the run-glicko command
    log "Skipping FIDE period validation for glicko_only mode with month range"
    VALID_FIDE_PERIOD=false
else
    YEAR=$(echo "$PROCESS_MONTH" | cut -d'-' -f1)
    MONTH=$(echo "$PROCESS_MONTH" | cut -d'-' -f2)

    # Remove leading zero from month for arithmetic operations
    MONTH_NUM=$((10#$MONTH))

    # Check if this is a valid FIDE rating period
    if is_valid_rating_period "$YEAR" "$MONTH_NUM"; then
        log "Month $PROCESS_MONTH is a valid FIDE rating period"
        VALID_FIDE_PERIOD=true
    else
        log "Month $PROCESS_MONTH is NOT a valid FIDE rating period"
        log "FIDE data processing steps (1-6) will be skipped"
        log "Glicko rating calculation (step 7) will still run"
        VALID_FIDE_PERIOD=false
    fi
fi

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
        log "Processed calculation data already exists for $PROCESS_MONTH ($processed_calc_count files)"
        log "The calculation processor will skip processing if data already exists"
    fi
    
    if [ -n "$missing_data" ]; then
        log "ERROR: Required data is missing for post-scraping mode:"
        echo -e "$missing_data"
        log ""
        log "Please run the full pipeline first, or use PIPELINE_MODE=full to run all steps."
        exit 1
    else
        log "All required data found for post-scraping mode"
        log "Found calculation files: $calc_count"
    fi
}

# Function to run full pipeline
run_full_pipeline() {
    log "Running full pipeline..."
    
    if [ "$VALID_FIDE_PERIOD" = true ]; then
        log "Processing FIDE data steps (1-6) for valid rating period $PROCESS_MONTH"
        
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
    else
        log "Skipping FIDE data processing steps (1-6) for invalid rating period $PROCESS_MONTH"
        log "Proceeding directly to Glicko rating calculation"
        run_glicko_only_pipeline
    fi
}

# Function to run post-scraping pipeline
run_post_scraping_pipeline() {
    log "Running post-scraping pipeline steps..."
    
    if [ "$VALID_FIDE_PERIOD" = true ]; then
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
    else
        log "Skipping calculation data processing (step 6) for invalid rating period $PROCESS_MONTH"
    fi

    run_glicko_only_pipeline
}

# Function to run glicko-only pipeline
run_glicko_only_pipeline() {
    log "Running Glicko steps..."
    
    # Step 7: Calculate Ratings
    log "Step 7: Calculating ratings with Glicko-2 algorithm..."
    log "Note: Glicko runs for ALL months, regardless of FIDE rating period validity"
    
    # Add debugging
    log "Checking if run-glicko is available..."
    which run-glicko || log "run-glicko not found in PATH"
    ls -la /app/bin/run-glicko || log "run-glicko binary not found at /app/bin/run-glicko"
    
    # Test if binary can execute at all
    log "Testing binary execution..."
    if ! /app/bin/run-glicko --help >/dev/null 2>&1; then
        log "ERROR: run-glicko binary cannot execute - checking dependencies..."
        ldd /app/bin/run-glicko || log "ldd failed"
        file /app/bin/run-glicko || log "file failed"
        exit 1
    fi
    
    log "Binary test passed, running with full parameters..."
    
    # Set Rust logging levels
    export RUST_LOG=info
    export RUST_BACKTRACE=1
    
    # Monitor the run
    set +e  # Don't exit on error
    
    start_time=$(date +%s)
    
    run-glicko \
        --month "$PROCESS_MONTH" \
        --s3-bucket "$S3_BUCKET" \
        --aws-region "$AWS_REGION"
    
    exit_code=$?
    end_time=$(date +%s)
    runtime=$((end_time - start_time))
    
    set -e  # Re-enable exit on error
    
    log "run-glicko completed in ${runtime} seconds with exit code: $exit_code"

    if [ $exit_code -ne 0 ]; then
        log "ERROR: Rating calculation failed with exit code $exit_code"
        exit 1
    fi

    log "Step 7 completed successfully"

    # Step 8: Upload Results (Future implementation)
    log "Step 8: Uploading results..."
    log "Results upload step not yet implemented"
    
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
Valid FIDE Rating Period: $VALID_FIDE_PERIOD

Steps completed based on mode and rating period validity:
EOF

        if [ "$mode" = "full" ]; then
            if [ "$VALID_FIDE_PERIOD" = true ]; then
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
            else
                cat >> /tmp/completion.txt << EOF
Steps 1-6: SKIPPED (invalid FIDE rating period)
7. Calculated ratings using Glicko-2 algorithm (Rust)
8. Uploaded results (placeholder)

Note: FIDE data processing steps were skipped because $PROCESS_MONTH is not a valid FIDE rating period
EOF
            fi
        elif [ "$mode" = "post_scraping" ]; then
            if [ "$VALID_FIDE_PERIOD" = true ]; then
                cat >> /tmp/completion.txt << EOF
6. Processed calculation data into compact binary format (Rust)
7. Calculated ratings using Glicko-2 algorithm (Rust)
8. Uploaded results (placeholder)

Note: Steps 1-5 were skipped (post-scraping mode)
EOF
            else
                cat >> /tmp/completion.txt << EOF
Step 6: SKIPPED (invalid FIDE rating period)
7. Calculated ratings using Glicko-2 algorithm (Rust)
8. Uploaded results (placeholder)

Note: Steps 1-6 were skipped (post-scraping mode + invalid FIDE rating period)
EOF
            fi
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

        if [ "$mode" = "full" ] && [ "$VALID_FIDE_PERIOD" = true ]; then
            cat >> /tmp/completion.txt << EOF
- s3://$S3_BUCKET/persistent/player_info/raw/$PROCESS_MONTH.txt
- s3://$S3_BUCKET/persistent/player_info/processed/$PROCESS_MONTH.txt
- s3://$S3_BUCKET/persistent/tournament_data/processed/[time_control]/$PROCESS_MONTH/[tournament_id].txt
- s3://$S3_BUCKET/persistent/active_players/${PROCESS_MONTH}_[time_control].txt
- s3://$S3_BUCKET/persistent/calculations/$PROCESS_MONTH/[time_control]/[player_id].json
EOF
        fi
        
        cat >> /tmp/completion.txt << EOF
- s3://$S3_BUCKET/persistent/ratings/$PROCESS_MONTH/[time_control].parquet
- s3://$S3_BUCKET/persistent/top_ratings/$PROCESS_MONTH/[time_control]/[category].json
- s3://$S3_BUCKET/results/$PROCESS_MONTH/rating_processing_completion.json

Rating Data Structure:
- Ratings are stored in Parquet format with rating, RD, and volatility values for each player
- Top rating lists contain ranked lists by category (open, women, juniors, girls) with player details
- All rating data uses Glicko-2 algorithm with proper uncertainty and volatility tracking
- Glicko ratings are calculated for ALL months regardless of FIDE data availability

FIDE Data Processing Logic:
- FIDE rating periods vary by year:
  * Before 2009: Quarterly (Jan, Apr, Jul, Oct)
  * 2009 (Jan-Aug): Quarterly (Jan, Apr, Jul)
  * 2009 (Sep-Dec): Bi-monthly (odd months: Sep, Nov)
  * 2010-2011: Bi-monthly (odd months only)
  * 2012 (Jan-Jul): Bi-monthly (odd months only)
  * 2012 (Aug-Dec): Monthly
  * 2013+: Monthly
EOF

        if [ "$VALID_FIDE_PERIOD" = false ]; then
            cat >> /tmp/completion.txt << EOF

Important: Month $PROCESS_MONTH is not a valid FIDE rating period.
FIDE data processing was skipped, but Glicko rating calculation was performed using existing data.
EOF
        fi
        
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
        if [ "$VALID_FIDE_PERIOD" = true ]; then
            log "Full Chess Rating Pipeline completed successfully for $PROCESS_MONTH"
        else
            log "Chess Rating Pipeline completed successfully for $PROCESS_MONTH (FIDE steps skipped - invalid rating period)"
        fi
        ;;
    "post_scraping")
        log "Starting post-scraping mode..."
        if [ "$VALID_FIDE_PERIOD" = true ]; then
            validate_post_scraping_data
        else
            log "WARNING: Post-scraping mode with invalid FIDE period - validation skipped"
        fi
        run_post_scraping_pipeline
        upload_completion_marker "post_scraping" "success"
        if [ "$VALID_FIDE_PERIOD" = true ]; then
            log "Post-scraping Chess Rating Pipeline completed successfully for $PROCESS_MONTH"
        else
            log "Post-scraping Chess Rating Pipeline completed successfully for $PROCESS_MONTH (FIDE steps skipped - invalid rating period)"
        fi
        ;;
    "glicko_only")
        log "Starting Glicko-only pipeline mode..."
        log "Note: Glicko-only mode runs for ALL months regardless of FIDE rating period validity"
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
    if [ "$VALID_FIDE_PERIOD" = true ]; then
        message="Chess Rating Pipeline ($PIPELINE_MODE mode) completed successfully for $PROCESS_MONTH."
        
        if [ "$PIPELINE_MODE" = "full" ]; then
            message="$message Downloaded player data, processed it, scraped/processed tournament data in parallel, aggregated unique player IDs by time control, scraped individual player calculation data, processed calculations into binary format, and calculated ratings using Glicko-2 algorithm."
        elif [ "$PIPELINE_MODE" = "post_scraping" ]; then
            message="$message Processed calculation data into binary format and calculated ratings using Glicko-2 algorithm."
        else
            message="$message Calculated ratings using Glicko-2 algorithm only."
        fi
    else
        message="Chess Rating Pipeline ($PIPELINE_MODE mode) completed successfully for $PROCESS_MONTH. Note: FIDE data processing steps were skipped because $PROCESS_MONTH is not a valid FIDE rating period. Only Glicko rating calculation was performed."
    fi
    
    aws sns publish \
        --topic-arn "$SNS_TOPIC_ARN" \
        --message "$message" \
        --subject "Chess Pipeline Success ($PIPELINE_MODE) - $PROCESS_MONTH" \
        --region "$AWS_REGION" || log "Warning: Failed to send SNS notification"
fi