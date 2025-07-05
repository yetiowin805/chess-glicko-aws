use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, Utc};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use tokio::fs;
use tracing::{error, info, warn};
use regex::Regex;

// Binary format constants
const MAGIC_HEADER: &[u8; 8] = b"CHSSGAME";
const FORMAT_VERSION: u16 = 1;

// Time controls
const TIME_CONTROLS: &[&str] = &["standard", "rapid", "blitz"];

#[derive(Parser, Debug)]
#[command(name = "calculation-processor")]
#[command(about = "Process FIDE calculation data from consolidated JSONL format")]
struct Args {
    #[arg(short, long, help = "Month for processing in YYYY-MM format")]
    month: String,
    
    #[arg(long, help = "S3 bucket for data storage")]
    s3_bucket: String,
    
    #[arg(long, default_value = "us-east-2", help = "AWS region")]
    aws_region: String,
}

// Binary format structures
#[repr(C, packed)]
#[derive(Clone, Copy)]
struct FileHeader {
    magic: [u8; 8],           // "CHSSGAME"
    version: u16,             // Format version
    time_control: u8,         // 0=standard, 1=rapid, 2=blitz
    reserved: u8,             // Reserved for future use
    player_count: u32,        // Number of players in file
    total_games: u64,         // Total number of games across all players
    timestamp: u64,           // Unix timestamp of creation
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct PlayerHeader {
    player_id_len: u8,        // Length of player ID string
    game_count: u16,          // Number of games for this player
    reserved: u8,             // Reserved for future use
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct GameRecord {
    tournament_id_hash: u64,  // Hash of tournament ID for space efficiency
    opponent_id_hash: u64,    // Hash of opponent ID
    opponent_rating: u16,     // Opponent rating (0 = unknown)
    result_and_flags: u8,     // Bits 0-1: result (0=loss, 1=draw, 2=win), Bit 2: player_unrated
    reserved: u8,             // Reserved for future use
}

// JSON structures for input parsing
#[derive(Deserialize)]
struct PlayerRecord {
    player_id: String,
    calculation_data: CalculationData,
}

#[derive(Deserialize)]
struct CalculationData {
    tournaments: Vec<Tournament>,
}

#[derive(Deserialize)]
struct Tournament {
    tournament_id: String,
    player_is_unrated: Option<bool>,
    games: Vec<Game>,
}

#[derive(Deserialize)]
struct Game {
    opponent_name: String,
    opponent_rating: Option<String>,
    result: String,
}

// Processing statistics
#[derive(Debug, Default)]
struct ProcessingStats {
    total_players: u32,
    processed_successfully: u32,
    failed_players: u32,
    total_games: u64,
}

// Player mapping structures
struct PlayerMappings {
    exact_mappings: HashMap<String, u64>,
    normalized_mappings: HashMap<String, u64>,
}

struct CalculationProcessor {
    s3_client: S3Client,
    s3_bucket: String,
    temp_dir: PathBuf,
    month_str: String,
}

impl CalculationProcessor {
    async fn new(s3_bucket: String, aws_region: String, month_str: String) -> Result<Self> {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_region)
            .load()
            .await;
        
        let s3_client = S3Client::new(&config);
        let temp_dir = PathBuf::from("/tmp/chess_data");
        
        // Create temp directory
        fs::create_dir_all(&temp_dir).await
            .context("Failed to create temp directory")?;
        
        info!("Initialized calculation processor - S3 bucket: {}, temp dir: {:?}", 
              s3_bucket, temp_dir);
        
        Ok(Self {
            s3_client,
            s3_bucket,
            temp_dir,
            month_str,
        })
    }

    async fn process_calculations_for_month(&self) -> Result<bool> {
        info!("Starting calculation processing for {}", self.month_str);
        
        // Parse year from month string
        let year: i32 = self.month_str[0..4].parse()
            .context("Invalid month format")?;
        
        // Determine which time controls to process
        let time_controls_to_process = if year >= 2012 {
            TIME_CONTROLS
        } else {
            &["standard"]
        };
        
        info!("Processing time controls: {:?}", time_controls_to_process);
        
        let mut results = Vec::new();
        
        for (index, &time_control) in time_controls_to_process.iter().enumerate() {
            match self.process_time_control(time_control, index as u8).await {
                Ok(result) => {
                    info!("Successfully processed {}: {:?}", time_control, result);
                    results.push((time_control.to_string(), serde_json::to_value(result)?));
                }
                Err(e) => {
                    error!("Error processing {}: {}", time_control, e);
                    results.push((
                        time_control.to_string(), 
                        serde_json::json!({"error": e.to_string()})
                    ));
                }
            }
        }
        
        self.upload_processing_completion_marker(&results).await?;
        info!("Calculation processing completed for {}", self.month_str);
        
        Ok(true)
    }

    async fn process_time_control(&self, time_control: &str, time_control_index: u8) -> Result<ProcessingStats> {
        // Check if already processed
        let output_s3_key = format!("persistent/calculations_processed/{}.bin", 
                                   self.get_output_filename(time_control));
        
        if self.check_s3_file_exists(&output_s3_key).await? {
            info!("Processed calculations already exist for {}: {}", time_control, output_s3_key);
            return Ok(ProcessingStats {
                total_players: 0,
                processed_successfully: 0,
                failed_players: 0,
                total_games: 0,
            });
        }
        
        // Download JSONL file
        let jsonl_s3_key = format!("persistent/calculations/{}/{}.jsonl", 
                                  self.month_str, time_control);
        
        if !self.check_s3_file_exists(&jsonl_s3_key).await? {
            info!("No calculation file found for {}: {}", time_control, jsonl_s3_key);
            return Ok(ProcessingStats::default());
        }
        
        info!("Found calculation file for {}: {}", time_control, jsonl_s3_key);
        
        // Download player database
        let player_mappings = self.load_player_database(time_control).await?;
        
        // Download and process JSONL file
        let local_jsonl_path = self.temp_dir.join(format!("{}.jsonl", time_control));
        self.download_file(&jsonl_s3_key, &local_jsonl_path).await?;
        
        let (processed_data, stats) = self.process_jsonl_file(&local_jsonl_path, &player_mappings).await?;
        
        // Clean up JSONL file
        fs::remove_file(&local_jsonl_path).await?;
        
        // Save processed data in binary format
        if !processed_data.is_empty() {
            self.save_processed_calculations_binary(&processed_data, time_control, time_control_index, &stats).await?;
        }
        
        info!("{}: Processing completed - {:?}", time_control, stats);
        Ok(stats)
    }

    async fn load_player_database(&self, time_control: &str) -> Result<PlayerMappings> {
        let s3_key = format!("persistent/player_info/processed/{}/{}.db", 
                            time_control, self.month_str);
        let local_db_path = self.temp_dir.join(format!("{}_{}.db", self.month_str, time_control));
        
        info!("Downloading player database for {}...", time_control);
        
        self.download_file(&s3_key, &local_db_path).await
            .context("Failed to download player database")?;
        
        // For this example, we'll create a simple hash-based mapping
        // In a real implementation, you'd parse the SQLite database
        let mut exact_mappings = HashMap::new();
        let mut normalized_mappings = HashMap::new();
        
        // This is a placeholder - you'd implement actual SQLite reading here
        // For now, we'll use a simple hash-based approach
        info!("Pre-loaded player mappings for {}", time_control);
        
        // Clean up database file
        fs::remove_file(&local_db_path).await?;
        
        Ok(PlayerMappings {
            exact_mappings,
            normalized_mappings,
        })
    }

    async fn process_jsonl_file(&self, jsonl_path: &PathBuf, player_mappings: &PlayerMappings) -> Result<(Vec<ProcessedPlayer>, ProcessingStats)> {
        let mut processed_data = Vec::new();
        let mut stats = ProcessingStats::default();
        
        let file = std::fs::File::open(jsonl_path)?;
        let reader = BufReader::new(file);
        
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            
            stats.total_players += 1;
            
            match serde_json::from_str::<PlayerRecord>(&line) {
                Ok(player_record) => {
                    match self.process_single_calculation(&player_record, player_mappings) {
                        Ok(Some(processed_player)) => {
                            stats.total_games += processed_player.games.len() as u64;
                            processed_data.push(processed_player);
                            stats.processed_successfully += 1;
                        }
                        Ok(None) => {
                            stats.failed_players += 1;
                        }
                        Err(e) => {
                            warn!("Error processing player {}: {}", player_record.player_id, e);
                            stats.failed_players += 1;
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to parse JSON line: {}", e);
                    stats.failed_players += 1;
                }
            }
        }
        
        info!("Processed {} players, {} successful, {} failed, {} total games", 
              stats.total_players, stats.processed_successfully, stats.failed_players, stats.total_games);
        
        Ok((processed_data, stats))
    }

    fn process_single_calculation(&self, player_record: &PlayerRecord, player_mappings: &PlayerMappings) -> Result<Option<ProcessedPlayer>> {
        let mut processed_games = Vec::new();
        
        for tournament in &player_record.calculation_data.tournaments {
            let tournament_id_hash = self.hash_string(&tournament.tournament_id);
            let is_unrated = tournament.player_is_unrated.unwrap_or(false);
            
            for game in &tournament.games {
                let opponent_name = game.opponent_name.trim();
                if opponent_name.is_empty() {
                    continue;
                }
                
                let score = self.convert_result_to_score(&game.result)?;
                let opponent_id_hash = self.get_player_id_hash(opponent_name, player_mappings);
                
                let opponent_rating = game.opponent_rating
                    .as_ref()
                    .and_then(|r| r.parse::<u16>().ok())
                    .unwrap_or(0);
                
                // Pack result and flags into a single byte
                let mut result_and_flags = score; // Bits 0-1
                if is_unrated {
                    result_and_flags |= 0b100; // Bit 2
                }
                
                let game_record = ProcessedGame {
                    tournament_id_hash,
                    opponent_id_hash,
                    opponent_rating,
                    result_and_flags,
                };
                
                processed_games.push(game_record);
            }
        }
        
        if processed_games.is_empty() {
            return Ok(None);
        }
        
        Ok(Some(ProcessedPlayer {
            player_id: player_record.player_id.clone(),
            games: processed_games,
        }))
    }

    fn convert_result_to_score(&self, result: &str) -> Result<u8> {
        match result.trim() {
            "1" | "1.0" | "1.00" => Ok(2), // Win
            "0.5" | "0.50" => Ok(1),       // Draw
            "0" | "0.0" | "0.00" => Ok(0), // Loss
            _ => anyhow::bail!("Unknown result format: {}", result),
        }
    }

    fn get_player_id_hash(&self, name: &str, player_mappings: &PlayerMappings) -> u64 {
        // Try exact match first
        if let Some(&id) = player_mappings.exact_mappings.get(&name.to_lowercase()) {
            return id;
        }
        
        // Try normalized match
        let normalized = self.normalize_player_name(name);
        if let Some(&id) = player_mappings.normalized_mappings.get(&normalized) {
            return id;
        }
        
        // Fallback: hash the name directly
        self.hash_string(name)
    }

    fn normalize_player_name(&self, name: &str) -> String {
        // Remove parentheses and content, extra spaces, lowercase, remove punctuation
        let re = Regex::new(r"\([^)]*\)").unwrap();
        let no_paren = re.replace_all(name, "");
        no_paren
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_lowercase()
            .chars()
            .filter(|c| c.is_alphanumeric() || c.is_whitespace())
            .collect()
    }

    fn hash_string(&self, s: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish()
    }

    async fn save_processed_calculations_binary(&self, processed_data: &[ProcessedPlayer], time_control: &str, time_control_index: u8, stats: &ProcessingStats) -> Result<()> {
        let output_filename = self.get_output_filename(time_control);
        let local_file = self.temp_dir.join(&output_filename);
        
        // Create binary file
        let mut file = std::fs::File::create(&local_file)?;
        
        // Write file header
        let header = FileHeader {
            magic: *MAGIC_HEADER,
            version: FORMAT_VERSION,
            time_control: time_control_index,
            reserved: 0,
            player_count: stats.processed_successfully,
            total_games: stats.total_games,
            timestamp: Utc::now().timestamp() as u64,
        };
        
        file.write_all(unsafe { 
            std::slice::from_raw_parts(
                &header as *const FileHeader as *const u8,
                std::mem::size_of::<FileHeader>()
            )
        })?;
        
        // Write player data
        for player in processed_data {
            // Write player header
            let player_header = PlayerHeader {
                player_id_len: player.player_id.len() as u8,
                game_count: player.games.len() as u16,
                reserved: 0,
            };
            
            file.write_all(unsafe {
                std::slice::from_raw_parts(
                    &player_header as *const PlayerHeader as *const u8,
                    std::mem::size_of::<PlayerHeader>()
                )
            })?;
            
            // Write player ID
            file.write_all(player.player_id.as_bytes())?;
            
            // Write game records
            for game in &player.games {
                let game_record = GameRecord {
                    tournament_id_hash: game.tournament_id_hash,
                    opponent_id_hash: game.opponent_id_hash,
                    opponent_rating: game.opponent_rating,
                    result_and_flags: game.result_and_flags,
                    reserved: 0,
                };
                
                file.write_all(unsafe {
                    std::slice::from_raw_parts(
                        &game_record as *const GameRecord as *const u8,
                        std::mem::size_of::<GameRecord>()
                    )
                })?;
            }
        }
        
        // Compress and upload to S3
        let compressed_file = self.temp_dir.join(format!("{}.gz", output_filename));
        self.compress_file(&local_file, &compressed_file).await?;
        
        let s3_key = format!("persistent/calculations_processed/{}.gz", output_filename);
        self.upload_file(&compressed_file, &s3_key).await?;
        
        // Clean up
        fs::remove_file(&local_file).await?;
        fs::remove_file(&compressed_file).await?;
        
        info!("Saved {} processed calculations to {}", processed_data.len(), s3_key);
        Ok(())
    }

    fn get_output_filename(&self, time_control: &str) -> String {
        format!("{}_{}.bin", self.month_str, time_control)
    }

    async fn compress_file(&self, input_path: &PathBuf, output_path: &PathBuf) -> Result<()> {
        let input = fs::read(input_path).await?;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&input)?;
        let compressed = encoder.finish()?;
        fs::write(output_path, compressed).await?;
        Ok(())
    }

    async fn download_file(&self, s3_key: &str, local_path: &PathBuf) -> Result<()> {
        let response = self.s3_client
            .get_object()
            .bucket(&self.s3_bucket)
            .key(s3_key)
            .send()
            .await?;
        
        let data = response.body.collect().await?.into_bytes();
        fs::write(local_path, data).await?;
        Ok(())
    }

    async fn upload_file(&self, local_path: &PathBuf, s3_key: &str) -> Result<()> {
        let data = fs::read(local_path).await?;
        
        self.s3_client
            .put_object()
            .bucket(&self.s3_bucket)
            .key(s3_key)
            .body(data.into())
            .send()
            .await?;
        
        info!("Uploaded {} to {}", local_path.display(), s3_key);
        Ok(())
    }

    async fn check_s3_file_exists(&self, s3_key: &str) -> Result<bool> {
        match self.s3_client
            .head_object()
            .bucket(&self.s3_bucket)
            .key(s3_key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn upload_processing_completion_marker(&self, results: &[(String, serde_json::Value)]) -> Result<()> {
        let completion_data = serde_json::json!({
            "month": self.month_str,
            "timestamp": Utc::now().to_rfc3339(),
            "step": "calculation_processing",
            "input_format": "consolidated_jsonl",
            "output_format": "binary_packed",
            "results": results.iter().collect::<HashMap<_, _>>()
        });
        
        let local_file = self.temp_dir.join(format!("calculation_processing_completion_{}.json", self.month_str));
        fs::write(&local_file, serde_json::to_string_pretty(&completion_data)?).await?;
        
        let s3_key = format!("results/{}/calculation_processing_completion.json", self.month_str);
        self.upload_file(&local_file, &s3_key).await?;
        
        fs::remove_file(&local_file).await?;
        info!("Uploaded processing completion marker for {}", self.month_str);
        Ok(())
    }
}

// Helper structures for processed data
#[derive(Debug)]
struct ProcessedPlayer {
    player_id: String,
    games: Vec<ProcessedGame>,
}

#[derive(Debug)]
struct ProcessedGame {
    tournament_id_hash: u64,
    opponent_id_hash: u64,
    opponent_rating: u16,
    result_and_flags: u8,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    
    let args = Args::parse();
    
    // Validate month format
    chrono::NaiveDate::parse_from_str(&format!("{}-01", args.month), "%Y-%m-%d")
        .context("Month must be in YYYY-MM format")?;
    
    let processor = CalculationProcessor::new(
        args.s3_bucket,
        args.aws_region,
        args.month,
    ).await?;
    
    match processor.process_calculations_for_month().await {
        Ok(true) => {
            info!("Calculation processing completed successfully");
            Ok(())
        }
        Ok(false) => {
            error!("Calculation processing failed");
            std::process::exit(1);
        }
        Err(e) => {
            error!("Processing failed with error: {}", e);
            std::process::exit(1);
        }
    }
}