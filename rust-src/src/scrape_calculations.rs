use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use aws_types::region::Region;
use clap::Parser;
use futures::{stream, StreamExt};
use regex::Regex;
use reqwest::Client;
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use serde_json;
use tokio::fs;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::{error, info, warn};

#[derive(Parser)]
#[command(name = "calculation-scraper")]
#[command(about = "Scrape FIDE player calculation data")]
struct Args {
    #[arg(long, help = "Month for processing in YYYY-MM format")]
    month: String,
    
    #[arg(long, help = "S3 bucket for data storage")]
    s3_bucket: String,
    
    #[arg(long, default_value = "us-east-2", help = "AWS region")]
    aws_region: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Game {
    opponent_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    opponent_rating: Option<String>,
    federation: String,
    result: String,
    tournament_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Tournament {
    tournament_id: String,
    player_is_unrated: bool,
    games: Vec<Game>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CalculationData {
    tournaments: Vec<Tournament>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PlayerCalculationRecord {
    player_id: String,
    calculation_data: CalculationData,
}

struct PlayerCalculationScraper {
    s3_client: S3Client,
    http_client: Client,
    s3_bucket: String,
    local_temp_dir: String,
    semaphore: Arc<Semaphore>,
}

impl PlayerCalculationScraper {
    async fn new(s3_bucket: String, aws_region: String) -> Result<Self> {
        let config = aws_config::from_env()
            .region(Region::new(aws_region.clone()))
            .load()
            .await;

        let s3_client = S3Client::new(&config);

        // Initialize HTTP client with conservative settings
        let http_client = Client::builder()
            .pool_max_idle_per_host(30)
            .pool_idle_timeout(Duration::from_secs(60))
            .timeout(Duration::from_secs(45))
            .user_agent("Mozilla/5.0 (compatible; FIDEScraper/1.0)")
            .build()?;

        let local_temp_dir = "/tmp/chess_data".to_string();
        let max_concurrent_requests = 40; // Aggressive but not excessive
        let semaphore = Arc::new(Semaphore::new(max_concurrent_requests));

        info!("Initialized calculation scraper - S3 bucket: {}, Max concurrent: {}", 
              s3_bucket, max_concurrent_requests);

        Ok(Self {
            s3_client,
            http_client,
            s3_bucket,
            local_temp_dir,
            semaphore,
        })
    }

    async fn file_exists_in_s3(&self, key: &str) -> Result<bool> {
        match self.s3_client.head_object().bucket(&self.s3_bucket).key(key).send().await {
            Ok(_) => Ok(true),
            Err(err) => {
                // Check if it's a "not found" error vs actual error
                if let Some(service_err) = err.as_service_error() {
                    if service_err.is_not_found() {
                        return Ok(false);
                    }
                }
                // For other errors, we should probably fail gracefully and continue
                warn!("Error checking if file exists in S3 ({}): {}", key, err);
                Ok(false)
            }
        }
    }

    async fn scrape_calculations_for_month(&self, month_str: &str) -> Result<()> {
        let parts: Vec<&str> = month_str.split('-').collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!("Invalid month format. Expected YYYY-MM"));
        }
        
        let year: u32 = parts[0].parse()?;
        let month: u32 = parts[1].parse()?;

        info!("Starting calculation scraping for {}-{:02}", year, month);

        // Check if completion marker already exists
        let completion_marker_key = format!("results/{}/calculation_scraping_completion.json", month_str);
        if self.file_exists_in_s3(&completion_marker_key).await? {
            info!("Completion marker already exists ({}). Skipping entire month processing.", completion_marker_key);
            return Ok(());
        }

        // Create temp directory
        fs::create_dir_all(&self.local_temp_dir).await?;

        // Determine time controls
        let mut time_controls = vec!["standard"];
        if year >= 2012 {
            time_controls.extend_from_slice(&["rapid", "blitz"]);
        }

        info!("Processing time controls: {:?}", time_controls);

        // Process time controls concurrently
        let tasks: Vec<_> = time_controls
            .into_iter()
            .map(|tc| self.process_time_control(tc, year, month))
            .collect();

        let results = futures::future::join_all(tasks).await;

        // Process results
        let mut successful = Vec::new();
        let mut failed = Vec::new();
        let mut skipped = Vec::new();

        for (i, result) in results.into_iter().enumerate() {
            let tc = ["standard", "rapid", "blitz"][i];
            match result {
                Ok(Some((processed, success_count))) => {
                    successful.push((tc.to_string(), processed, success_count));
                }
                Ok(None) => {
                    skipped.push(tc.to_string());
                }
                Err(e) => {
                    failed.push((tc.to_string(), e.to_string()));
                }
            }
        }

        let total_processed: usize = successful.iter().map(|(_, p, _)| p).sum();
        let total_successful: usize = successful.iter().map(|(_, _, s)| s).sum();

        info!("Results - Time controls: {} successful, {} failed, {} skipped", 
              successful.len(), failed.len(), skipped.len());
        info!("Total calculations: {} processed, {} successful", total_processed, total_successful);

        if !failed.is_empty() {
            error!("Failed time controls: {:?}", failed);
        }
        if !skipped.is_empty() {
            info!("Skipped time controls (already exist): {:?}", skipped);
        }

        // Only upload completion marker if we actually processed something or had failures
        if !successful.is_empty() || !failed.is_empty() {
            self.upload_completion_marker(month_str, &successful, &failed).await?;
        } else {
            info!("All time controls were skipped - completion marker already exists or no work needed");
        }
        Ok(())
    }

    async fn process_time_control(&self, time_control: &str, year: u32, month: u32) -> Result<Option<(usize, usize)>> {
        // Check if output file already exists
        let output_key = format!("persistent/calculations/{}-{:02}/{}.jsonl", year, month, time_control);
        if self.file_exists_in_s3(&output_key).await? {
            info!("{}: Output file already exists ({}). Skipping processing.", time_control, output_key);
            return Ok(None);
        }

        let player_ids = self.download_active_players(time_control, year, month).await?;

        if player_ids.is_empty() {
            info!("No active players found for {} {}-{:02}", time_control, year, month);
            return Ok(Some((0, 0)));
        }

        info!("Processing {} players for {}", player_ids.len(), time_control);

        // Create JSONL file for this time control
        let jsonl_file_path = format!("{}/calculations_{}-{:02}_{}.jsonl", 
                                     self.local_temp_dir, year, month, time_control);
        let mut jsonl_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&jsonl_file_path)
            .await?;

        let batch_size = 80; // Larger batches for better performance
        let mut total_processed = 0;
        let mut total_successful = 0;

        for (batch_num, chunk) in player_ids.chunks(batch_size).enumerate() {
            let batch_num = batch_num + 1;
            info!("{}: Processing batch {} ({} players)", time_control, batch_num, chunk.len());

            let results: Vec<Option<PlayerCalculationRecord>> = stream::iter(chunk)
                .map(|player_id| self.process_single_player(player_id, time_control, year, month))
                .buffer_unordered(40) // High concurrency within batch
                .collect::<Vec<Result<Option<PlayerCalculationRecord>>>>()
                .await
                .into_iter()
                .map(|r| r.unwrap_or(None))
                .collect();

            let mut batch_successful = 0;
            for result in results {
                total_processed += 1;
                if let Some(record) = result {
                    // Write to JSONL file
                    let json_line = serde_json::to_string(&record)?;
                    jsonl_file.write_all(json_line.as_bytes()).await?;
                    jsonl_file.write_all(b"\n").await?;
                    batch_successful += 1;
                    total_successful += 1;
                }
            }

            info!("{}: Batch {} - {}/{} successful, Total: {}/{}", 
                  time_control, batch_num, batch_successful, chunk.len(), 
                  total_successful, total_processed);

            // Minimal pause to be respectful
            sleep(Duration::from_millis(200)).await;
        }

        // Close the file
        jsonl_file.flush().await?;
        drop(jsonl_file);

        // Upload the consolidated JSONL file to S3
        if total_successful > 0 {
            self.upload_jsonl_file(&jsonl_file_path, time_control, year, month).await?;
        }

        // Clean up local file
        fs::remove_file(&jsonl_file_path).await?;

        info!("{}: Completed - {}/{} players successful", time_control, total_successful, total_processed);
        Ok(Some((total_processed, total_successful)))
    }

    async fn upload_jsonl_file(&self, local_file_path: &str, time_control: &str, year: u32, month: u32) -> Result<()> {
        let file_content = fs::read(local_file_path).await?;
        let s3_key = format!("persistent/calculations/{}-{:02}/{}.jsonl", year, month, time_control);
        
        self.s3_client
            .put_object()
            .bucket(&self.s3_bucket)
            .key(&s3_key)
            .body(file_content.into())
            .send()
            .await?;

        info!("Uploaded JSONL file: {}", s3_key);
        Ok(())
    }

    async fn download_active_players(&self, time_control: &str, year: u32, month: u32) -> Result<Vec<String>> {
        let s3_key = format!("persistent/active_players/{}-{:02}_{}.txt", year, month, time_control);

        match self.s3_client.get_object().bucket(&self.s3_bucket).key(&s3_key).send().await {
            Ok(response) => {
                let body = response.body.collect().await?;
                let content = String::from_utf8(body.to_vec())?;
                let player_ids: Vec<String> = content
                    .lines()
                    .map(|line| line.trim().to_string())
                    .filter(|line| !line.is_empty())
                    .collect();
                Ok(player_ids)
            }
            Err(_) => {
                warn!("No active players file found for {} {}-{:02}", time_control, year, month);
                Ok(Vec::new())
            }
        }
    }

    async fn process_single_player(&self, player_id: &str, time_control: &str, year: u32, month: u32) -> Result<Option<PlayerCalculationRecord>> {
        let tc_code = match time_control {
            "standard" => "0",
            "rapid" => "1", 
            "blitz" => "2",
            _ => return Err(anyhow::anyhow!("Invalid time control: {}", time_control)),
        };

        let url = format!(
            "https://ratings.fide.com/a_indv_calculations.php?id_number={}&rating_period={}-{:02}-01&t={}",
            player_id, year, month, tc_code
        );

        let _permit = self.semaphore.acquire().await?;

        // Single attempt with good timeout
        match self.http_client.get(&url).send().await {
            Ok(response) if response.status().is_success() => {
                let html_content = response.text().await?;

                // Check for no data conditions
                if html_content.contains("No calculations available") ||
                   html_content.contains("No games") ||
                   html_content.trim().len() < 100 {
                    return Ok(None); // Normal case - no data
                }

                // Extract data
                let calculation_data = self.extract_calculation_data(&html_content)?;
                if !calculation_data.tournaments.is_empty() {
                    Ok(Some(PlayerCalculationRecord {
                        player_id: player_id.to_string(),
                        calculation_data,
                    }))
                } else {
                    Ok(None)
                }
            }
            _ => Ok(None), // Failed but don't retry to maintain speed
        }
    }

    fn extract_calculation_data(&self, html_content: &str) -> Result<CalculationData> {
        let document = Html::parse_document(html_content);
        let mut tournaments = Vec::new();
        let mut tournament_ids = Vec::new();

        // Extract tournament IDs
        let header_selector = Selector::parse("div.rtng_line01").unwrap();
        let link_selector = Selector::parse("a").unwrap();
        let event_regex = Regex::new(r"event=(\d+)").unwrap();

        for header in document.select(&header_selector) {
            if let Some(link) = header.select(&link_selector).next() {
                if let Some(href) = link.value().attr("href") {
                    if let Some(captures) = event_regex.captures(href) {
                        if let Some(tournament_id) = captures.get(1) {
                            tournament_ids.push(tournament_id.as_str().to_string());
                        }
                    }
                }
            }
        }

        // Extract game data
        let table_selector = Selector::parse("table.calc_table").unwrap();
        let row_selector = Selector::parse("tr[bgcolor='#efefef']").unwrap();
        let cell_selector = Selector::parse("td.list4").unwrap();
        let rating_regex = Regex::new(r"-?\d+").unwrap();

        for (i, table) in document.select(&table_selector).enumerate() {
            if i >= tournament_ids.len() { break; }

            let tournament_id = &tournament_ids[i];
            let mut games = Vec::new();

            // Check if unrated
            let player_is_unrated = table.inner_html().contains("Rp");

            for row in table.select(&row_selector) {
                let cells: Vec<_> = row.select(&cell_selector).collect();
                if cells.len() < 6 { continue; }

                let opponent_name = cells[0].text().collect::<String>().trim().to_string();
                let opponent_rating = if cells.len() > 3 {
                    let rating_text = cells[3].text().collect::<String>();
                    rating_regex.find(&rating_text).map(|m| m.as_str().to_string())
                } else { None };
                let federation = if cells.len() > 4 { 
                    cells[4].text().collect::<String>().trim().to_string() 
                } else { String::new() };
                let result = if cells.len() > 5 { 
                    cells[5].text().collect::<String>().trim().to_string() 
                } else { String::new() };

                games.push(Game {
                    opponent_name,
                    opponent_rating,
                    federation,
                    result,
                    tournament_id: tournament_id.clone(),
                });
            }

            if !games.is_empty() {
                tournaments.push(Tournament {
                    tournament_id: tournament_id.clone(),
                    player_is_unrated,
                    games,
                });
            }
        }

        Ok(CalculationData { tournaments })
    }

    async fn upload_completion_marker(&self, month_str: &str, successful: &[(String, usize, usize)], failed: &[(String, String)]) -> Result<()> {
        let mut statistics = HashMap::new();
        statistics.insert("time_controls_processed".to_string(), serde_json::Value::Number((successful.len() + failed.len()).into()));
        statistics.insert("time_controls_successful".to_string(), serde_json::Value::Number(successful.len().into()));
        statistics.insert("time_controls_failed".to_string(), serde_json::Value::Number(failed.len().into()));

        let mut details = HashMap::new();
        for (tc, processed, success_count) in successful {
            let mut tc_details = HashMap::new();
            tc_details.insert("processed".to_string(), serde_json::Value::Number((*processed).into()));
            tc_details.insert("successful".to_string(), serde_json::Value::Number((*success_count).into()));
            details.insert(tc.clone(), serde_json::Value::Object(tc_details.into_iter().collect()));
        }
        statistics.insert("details".to_string(), serde_json::Value::Object(details.into_iter().collect()));

        let completion_data = serde_json::json!({
            "month": month_str,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "statistics": statistics,
            "step": "calculation_scraping",
            "method": "rust_optimized_jsonl",
            "output_format": "consolidated_jsonl"
        });

        let local_file = format!("{}/calculation_scraping_completion_{}.json", self.local_temp_dir, month_str);
        let json_content = serde_json::to_string_pretty(&completion_data)?;
        fs::write(&local_file, &json_content).await?;

        let s3_key = format!("results/{}/calculation_scraping_completion.json", month_str);
        self.s3_client
            .put_object()
            .bucket(&self.s3_bucket)
            .key(&s3_key)
            .body(json_content.into_bytes().into())
            .send()
            .await?;

        fs::remove_file(&local_file).await?;
        info!("Uploaded completion marker for {}", month_str);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    info!("Starting Rust calculation scraper (JSONL output) for {}", args.month);

    let scraper = PlayerCalculationScraper::new(args.s3_bucket, args.aws_region).await?;
    scraper.scrape_calculations_for_month(&args.month).await?;

    info!("Calculation scraping completed successfully for {}", args.month);
    Ok(())
}