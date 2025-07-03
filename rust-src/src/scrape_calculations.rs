use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use clap::Parser;
use futures::{stream, StreamExt};
use regex::Regex;
use reqwest::Client;
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use serde_json;
use tokio::fs;
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

struct PlayerCalculationScraper {
    s3_client: S3Client,
    http_client: Client,
    s3_bucket: String,
    local_temp_dir: String,
    semaphore: Arc<Semaphore>,
}

impl PlayerCalculationScraper {
    async fn new(s3_bucket: String, aws_region: String) -> Result<Self> {
        // Initialize AWS SDK with older API
        let config = aws_config::from_env()
            .region(Region::new(&args.aws_region))
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

    async fn scrape_calculations_for_month(&self, month_str: &str) -> Result<()> {
        let parts: Vec<&str> = month_str.split('-').collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!("Invalid month format. Expected YYYY-MM"));
        }
        
        let year: u32 = parts[0].parse()?;
        let month: u32 = parts[1].parse()?;

        info!("Starting calculation scraping for {}-{:02}", year, month);

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

        for (i, result) in results.into_iter().enumerate() {
            let tc = ["standard", "rapid", "blitz"][i];
            match result {
                Ok((processed, success_count)) => {
                    successful.push((tc.to_string(), processed, success_count));
                }
                Err(e) => {
                    failed.push((tc.to_string(), e.to_string()));
                }
            }
        }

        let total_processed: usize = successful.iter().map(|(_, p, _)| p).sum();
        let total_successful: usize = successful.iter().map(|(_, _, s)| s).sum();

        info!("Results - Time controls: {} successful, {} failed", successful.len(), failed.len());
        info!("Total calculations: {} processed, {} successful", total_processed, total_successful);

        if !failed.is_empty() {
            error!("Failed time controls: {:?}", failed);
        }

        self.upload_completion_marker(month_str, &successful, &failed).await?;
        Ok(())
    }

    async fn process_time_control(&self, time_control: &str, year: u32, month: u32) -> Result<(usize, usize)> {
        let player_ids = self.download_active_players(time_control, year, month).await?;

        if player_ids.is_empty() {
            info!("No active players found for {} {}-{:02}", time_control, year, month);
            return Ok((0, 0));
        }

        info!("Processing {} players for {}", player_ids.len(), time_control);

        let batch_size = 80; // Larger batches for better performance
        let mut total_processed = 0;
        let mut total_successful = 0;

        for (batch_num, chunk) in player_ids.chunks(batch_size).enumerate() {
            let batch_num = batch_num + 1;
            info!("{}: Processing batch {} ({} players)", time_control, batch_num, chunk.len());

            let results: Vec<bool> = stream::iter(chunk)
                .map(|player_id| self.process_single_player(player_id, time_control, year, month))
                .buffer_unordered(40) // High concurrency within batch
                .collect::<Vec<Result<bool>>>()
                .await
                .into_iter()
                .map(|r| r.unwrap_or(false))
                .collect();

            let batch_successful = results.iter().filter(|&&r| r).count();
            total_processed += results.len();
            total_successful += batch_successful;

            info!("{}: Batch {} - {}/{} successful, Total: {}/{}", 
                  time_control, batch_num, batch_successful, results.len(), 
                  total_successful, total_processed);

            // Minimal pause to be respectful
            sleep(Duration::from_millis(200)).await;
        }

        info!("{}: Completed - {}/{} players successful", time_control, total_successful, total_processed);
        Ok((total_processed, total_successful))
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

    async fn process_single_player(&self, player_id: &str, time_control: &str, year: u32, month: u32) -> Result<bool> {
        let s3_key = format!("persistent/calculations/{}-{:02}/{}/{}.json", year, month, time_control, player_id);

        // Check if already processed
        if self.check_s3_file_exists(&s3_key).await.unwrap_or(false) {
            return Ok(true);
        }

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
                    return Ok(true); // Normal case
                }

                // Extract and save data
                let calculation_data = self.extract_calculation_data(&html_content)?;
                if !calculation_data.tournaments.is_empty() {
                    self.save_and_upload_calculation(calculation_data, player_id, time_control, year, month, &s3_key).await?;
                }
                Ok(true)
            }
            _ => Ok(false), // Failed but don't retry to maintain speed
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

    async fn save_and_upload_calculation(
        &self, calculation_data: CalculationData, player_id: &str, 
        time_control: &str, year: u32, month: u32, s3_key: &str,
    ) -> Result<()> {
        let local_dir = format!("{}/calculations/{}-{:02}/{}", self.local_temp_dir, year, month, time_control);
        fs::create_dir_all(&local_dir).await?;

        let local_file = format!("{}/{}.json", local_dir, player_id);
        let json_content = serde_json::to_string(&calculation_data)?;
        fs::write(&local_file, &json_content).await?;

        // Upload to S3
        self.s3_client
            .put_object()
            .bucket(&self.s3_bucket)
            .key(s3_key)
            .body(json_content.into_bytes().into())
            .send()
            .await?;

        fs::remove_file(&local_file).await?;
        Ok(())
    }

    async fn check_s3_file_exists(&self, s3_key: &str) -> Result<bool> {
        match self.s3_client.head_object().bucket(&self.s3_bucket).key(s3_key).send().await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
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
            "method": "rust_optimized"
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

    info!("Starting Rust calculation scraper for {}", args.month);

    let scraper = PlayerCalculationScraper::new(args.s3_bucket, args.aws_region).await?;
    scraper.scrape_calculations_for_month(&args.month).await?;

    info!("Calculation scraping completed successfully for {}", args.month);
    Ok(())
}