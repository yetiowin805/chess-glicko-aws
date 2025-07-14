use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;
use aws_types::region::Region;
use chrono::{Datelike, Utc};
use clap::Parser;
use futures::future;
use serde::Serialize;
use std::collections::HashMap;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs;
use tracing::{error, info, warn};
use rayon::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use rusqlite::Connection;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

// Parquet/Arrow imports
use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::WriterProperties;

// Binary format constants (matching calculation processor)
const MAGIC_HEADER: &[u8; 8] = b"CHSSGAME";
const FORMAT_VERSION: u16 = 1;

// Rating calculation constants
const BASE_RATING: f64 = 1500.0;
const BASE_RD: f64 = 350.0;
const BASE_VOLATILITY: f64 = 0.09;
const TAU: f64 = 0.2;
const SCALE: f64 = 173.7178;
const MAX_RD: f64 = 500.0;
const MAX_VOLATILITY: f64 = 0.1;
const EPS: f64 = 1e-6;
const PI_SQUARED: f64 = std::f64::consts::PI * std::f64::consts::PI;

#[derive(Parser, Debug)]
#[command(name = "run-glicko")]
#[command(about = "Process binary game data and update Glicko-2 ratings")]
struct Args {
    #[arg(short, long, help = "Month for processing in YYYY-MM format (mutually exclusive with --first-month/--last-month)")]
    month: Option<String>,
    
    #[arg(long, help = "First month for batch processing in YYYY-MM format")]
    first_month: Option<String>,
    
    #[arg(long, help = "Last month for batch processing in YYYY-MM format")]
    last_month: Option<String>,
    
    #[arg(long, help = "S3 bucket for data storage")]
    s3_bucket: String,
    
    #[arg(long, default_value = "us-east-2", help = "AWS region")]
    aws_region: String,
    
    #[arg(long, default_value = "4", help = "Number of worker threads for parallel processing")]
    workers: usize,
}

// Function to generate month range
fn generate_month_range(first_month: &str, last_month: &str) -> Result<Vec<String>> {
    let first_date = chrono::NaiveDate::parse_from_str(&format!("{}-01", first_month), "%Y-%m-%d")
        .context("First month must be in YYYY-MM format")?;
    let last_date = chrono::NaiveDate::parse_from_str(&format!("{}-01", last_month), "%Y-%m-%d")
        .context("Last month must be in YYYY-MM format")?;
    
    if first_date > last_date {
        anyhow::bail!("First month must be less than or equal to last month");
    }
    
    let mut months = Vec::new();
    let mut current_date = first_date;
    
    while current_date <= last_date {
        months.push(current_date.format("%Y-%m").to_string());
        
        // Move to next month
        current_date = if current_date.month() == 12 {
            chrono::NaiveDate::from_ymd_opt(current_date.year() + 1, 1, 1)
                .context("Failed to calculate next year")?
        } else {
            chrono::NaiveDate::from_ymd_opt(current_date.year(), current_date.month() + 1, 1)
                .context("Failed to calculate next month")?
        };
    }
    
    Ok(months)
}

// Function to validate month format
fn validate_month_format(month: &str) -> Result<()> {
    chrono::NaiveDate::parse_from_str(&format!("{}-01", month), "%Y-%m-%d")
        .context("Month must be in YYYY-MM format")?;
    Ok(())
}

// Binary format structures (matching calculation processor)
#[repr(C, packed)]
#[derive(Clone, Copy)]
struct FileHeader {
    magic: [u8; 8],
    version: u16,
    time_control: u8,
    reserved: u8,
    player_count: u32,
    total_games: u64,
    timestamp: u64,
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct PlayerHeader {
    player_id_len: u8,
    game_count: u16,
    reserved: u8,
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct GameRecord {
    tournament_id_hash: u64,
    opponent_id_hash: u64,
    opponent_rating: u16,
    result_and_flags: u8,
    reserved: u8,
}

// Rating structures
#[derive(Debug, Clone)]
struct Player {
    id: String,
    rating: f64,
    rd: f64,
    volatility: f64,
    new_rating: f64,
    new_rd: f64,
    new_volatility: f64,
    games: Vec<GameResult>,
}

#[derive(Debug, Clone)]
struct GameResult {
    opponent_rating: f64,
    opponent_rd: f64,
    score: f64,
}

#[derive(Debug, Clone, Serialize)]
struct PlayerInfo {
    id: String,
    name: String,
    federation: String,
    sex: String,
    birth_year: Option<u32>, // Changed to Option<u32> to handle NULL values
}

#[derive(Debug, Serialize)]
struct TopRatingEntry {
    rank: u32,
    name: String,
    federation: String,
    birth_year: Option<u32>, // Changed to Option<u32> to handle NULL values
    sex: Option<String>, // None for women/girls categories
    rating: f64,
    rd: f64,
    player_id: String,
}

#[derive(Debug, Default, Serialize)]
struct ProcessingStats {
    total_players: u32,
    processed_successfully: u32,
    failed_players: u32,
    total_games: u64,
    time_control: String,
}

#[derive(Clone)]
struct RatingProcessor {
    s3_client: S3Client,
    s3_bucket: String,
    temp_dir: PathBuf,
    month_str: String,
    year: i32,
    month: u32,
    workers: usize,
}

impl RatingProcessor {
    async fn new(s3_bucket: String, aws_region: String, month_str: String, workers: usize) -> Result<Self> {
        info!("Initializing processor for {} with {} workers", month_str, workers);

        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(aws_region.clone()))
            .load()
            .await;
        
        let s3_client = S3Client::new(&config);
        
        let temp_dir = PathBuf::from("/tmp/rating_data");
        fs::create_dir_all(&temp_dir).await
            .context("Failed to create temp directory")?;
        
        let year: i32 = month_str[0..4].parse()
            .context("Invalid month format")?;
        let month: u32 = month_str[5..7].parse()
            .context("Invalid month format")?;
        
        // Thread pool initialization moved to main function
        
        Ok(Self {
            s3_client,
            s3_bucket,
            temp_dir,
            month_str,
            year,
            month,
            workers,
        })
    }

    async fn process_ratings_for_month(&self) -> Result<()> {
        info!("Starting rating processing for {}", self.month_str);
        
        // Determine time controls to process based on year
        let time_controls = if self.year > 2012 || (self.year == 2012 && self.month >= 2) {
            vec!["standard", "rapid", "blitz"]
        } else {
            vec!["standard"]
        };
        
        info!("Processing {} time control(s): {:?}", time_controls.len(), time_controls);
        
        // Process time controls in parallel using Tokio tasks
        let tasks: Vec<_> = time_controls
            .into_iter()
            .map(|time_control| {
                let processor = self.clone();
                tokio::task::spawn(async move {
                    processor.process_time_control(&time_control).await
                })
            })
            .collect();
        
        // Wait for all tasks to complete
        let results = future::join_all(tasks).await;
        
        let mut processing_results = Vec::new();
        let mut has_errors = false;
        
        for task_result in results {
            match task_result {
                Ok(Ok(stats)) => {
                    info!("Completed {}: {} players, {} games", stats.time_control, stats.total_players, stats.total_games);
                    processing_results.push((stats.time_control.clone(), serde_json::to_value(stats)?));
                }
                Ok(Err(e)) => {
                    error!("Failed time control: {}", e);
                    has_errors = true;
                }
                Err(e) => {
                    error!("Task panicked: {}", e);
                    has_errors = true;
                }
            }
        }
        
        if has_errors {
            return Err(anyhow::anyhow!("Processing failed for some time controls"));
        }
        
        self.upload_processing_completion_marker(&processing_results).await?;
        info!("Rating processing completed successfully for {}", self.month_str);
        
        Ok(())
    }

    async fn process_time_control(&self, time_control: &str) -> Result<ProcessingStats> {
        info!("Processing time control: {}", time_control);
        
        // Load current ratings from previous month
        let mut players = self.load_current_ratings(time_control).await?;
        info!("Loaded {} existing player ratings", players.len());
        
        // Load and process games for this month
        let total_games_loaded = self.load_games_from_binary(time_control, &mut players).await?;
        info!("Loaded {} games for {}", total_games_loaded, time_control);
        
        if total_games_loaded == 0 {
            warn!("No games found for {}, applying RD decay only", time_control);
        }
        
        // Update ratings in parallel
        let stats = self.update_ratings_parallel(&mut players, time_control).await?;
        
        // Load player info for output
        let player_info = self.load_player_info(time_control).await?;
        
        // Save updated ratings as Parquet
        self.save_ratings_parquet(&players, time_control).await?;
        
        // Generate and save top rating lists as JSON
        self.generate_top_rating_lists(&players, &player_info, time_control).await?;
        
        info!("Completed {}: {} players processed", time_control, stats.total_players);
        Ok(stats)
    }

    async fn load_current_ratings(&self, time_control: &str) -> Result<HashMap<String, Player>> {
        // Try to load previous month's ratings
        let prev_month = self.calculate_previous_month(&self.month_str)?;
        let prev_ratings_key = format!("persistent/ratings/{}/{}.parquet", prev_month, time_control);
        
        if self.check_s3_file_exists(&prev_ratings_key).await? {
            info!("Loading previous ratings from {}", prev_month);
            return self.load_ratings_from_parquet(&prev_ratings_key).await;
        }
        
        // If no previous ratings, start with empty set
        warn!("No previous ratings found, starting fresh for {}", time_control);
        Ok(HashMap::new())
    }

    async fn load_ratings_from_parquet(&self, s3_key: &str) -> Result<HashMap<String, Player>> {
        let local_file = self.temp_dir.join(format!("previous_ratings_{}.parquet", 
            s3_key.replace("/", "_").replace(":", "")));
        
        self.download_file(s3_key, &local_file).await
            .context("Failed to download previous ratings file")?;
        
        let mut players = HashMap::new();
        
        // Read Parquet file using Arrow
        let file = std::fs::File::open(&local_file)
            .context("Failed to open local Parquet file")?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .context("Failed to create Parquet reader builder")?;
        let reader = builder.build()
            .context("Failed to build Parquet reader")?;
        
        let mut total_rows = 0;
        
        for batch_result in reader {
            let batch = batch_result.context("Failed to read batch from Parquet file")?;
            total_rows += batch.num_rows();
            
            let player_ids = batch.column(0).as_any().downcast_ref::<StringArray>()
                .context("Failed to read player_id column")?;
            let ratings = batch.column(1).as_any().downcast_ref::<Float64Array>()
                .context("Failed to read rating column")?;
            let rds = batch.column(2).as_any().downcast_ref::<Float64Array>()
                .context("Failed to read rd column")?;
            let volatilities = batch.column(3).as_any().downcast_ref::<Float64Array>()
                .context("Failed to read volatility column")?;
            
            for i in 0..batch.num_rows() {
                let id = player_ids.value(i).to_string();
                let rating = ratings.value(i);
                let rd = rds.value(i);
                let volatility = volatilities.value(i);
                
                players.insert(id.clone(), Player {
                    id,
                    rating,
                    rd,
                    volatility,
                    new_rating: rating,
                    new_rd: rd,
                    new_volatility: volatility,
                    games: Vec::new(),
                });
            }
        }
        
        fs::remove_file(&local_file).await
            .context("Failed to remove temporary Parquet file")?;
        Ok(players)
    }

    async fn load_games_from_binary(&self, time_control: &str, players: &mut HashMap<String, Player>) -> Result<u64> {
        let binary_s3_key = format!("persistent/calculations_processed/{}_{}.bin.gz", self.month_str, time_control);
        
        if !self.check_s3_file_exists(&binary_s3_key).await? {
            warn!("No games file found: {}", binary_s3_key);
            return Ok(0);
        }
        
        let compressed_file = self.temp_dir.join(format!("{}_{}.bin.gz", self.month_str, time_control));
        let binary_file = self.temp_dir.join(format!("{}_{}.bin", self.month_str, time_control));
        
        // Download and decompress
        self.download_file(&binary_s3_key, &compressed_file).await
            .context("Failed to download binary games file")?;
        
        self.decompress_file(&compressed_file, &binary_file).await
            .context("Failed to decompress binary file")?;
        
        // Clone the current players HashMap for opponent lookup (contains previous month's ratings)
        let previous_ratings = players.clone();
        
        // Read binary file
        let games_loaded = self.read_binary_games_file(&binary_file, players, &previous_ratings).await
            .context("Failed to read binary games file")?;
        
        // Cleanup
        fs::remove_file(&compressed_file).await
            .context("Failed to remove compressed file")?;
        fs::remove_file(&binary_file).await
            .context("Failed to remove binary file")?;
        
        Ok(games_loaded)
    }

    fn create_player_id_hash_map(&self, players: &HashMap<String, Player>) -> HashMap<u64, String> {
        let mut hash_map = HashMap::new();
        
        for player_id in players.keys() {
            let mut hasher = DefaultHasher::new();
            player_id.hash(&mut hasher);
            let hash = hasher.finish();
            hash_map.insert(hash, player_id.clone());
        }
        
        hash_map
    }

    async fn read_binary_games_file(&self, file_path: &PathBuf, players: &mut HashMap<String, Player>, previous_ratings: &HashMap<String, Player>) -> Result<u64> {
        let file = std::fs::File::open(file_path)
            .context("Failed to open binary games file")?;
        let mut reader = BufReader::new(file);
        
        // Read file header
        let mut header_bytes = [0u8; std::mem::size_of::<FileHeader>()];
        reader.read_exact(&mut header_bytes)
            .context("Failed to read file header")?;
        let header: FileHeader = unsafe { std::ptr::read(header_bytes.as_ptr() as *const FileHeader) };
        
        // Verify magic header and version
        if header.magic != *MAGIC_HEADER {
            anyhow::bail!("Invalid magic header in binary file");
        }
        
        // Copy packed struct fields to local variables to avoid unaligned access
        let version = header.version;
        let player_count = header.player_count;
        let total_games = header.total_games;
        
        if version != FORMAT_VERSION {
            anyhow::bail!("Unsupported file version: {}", version);
        }
        
        info!("Processing {} players with {} total games", player_count, total_games);
        
        // Create hash-to-player-ID mapping for opponent lookup
        let hash_to_player_id = self.create_player_id_hash_map(previous_ratings);
        info!("Created hash mapping for {} previous month players", hash_to_player_id.len());
        
        let mut total_games_loaded = 0u64;
        let mut opponents_not_found = 0u64;
        let mut opponents_found = 0u64;
        
        // Read player data
        for player_idx in 0..player_count {
            // Read player header
            let mut player_header_bytes = [0u8; std::mem::size_of::<PlayerHeader>()];
            reader.read_exact(&mut player_header_bytes)
                .context("Failed to read player header")?;
            let player_header: PlayerHeader = unsafe { 
                std::ptr::read(player_header_bytes.as_ptr() as *const PlayerHeader) 
            };
            
            // Read player ID
            let mut player_id_bytes = vec![0u8; player_header.player_id_len as usize];
            reader.read_exact(&mut player_id_bytes)
                .context("Failed to read player ID")?;
            let player_id = String::from_utf8(player_id_bytes)
                .context("Failed to parse player ID as UTF-8")?;
            
            // Get or create player
            let player = players.entry(player_id.clone()).or_insert_with(|| Player {
                id: player_id.clone(),
                rating: BASE_RATING,
                rd: BASE_RD,
                volatility: BASE_VOLATILITY,
                new_rating: BASE_RATING,
                new_rd: BASE_RD,
                new_volatility: BASE_VOLATILITY,
                games: Vec::new(),
            });
            
            // Read games for this player
            for _game_idx in 0..player_header.game_count {
                let mut game_bytes = [0u8; std::mem::size_of::<GameRecord>()];
                reader.read_exact(&mut game_bytes)
                    .context("Failed to read game record")?;
                let game_record: GameRecord = unsafe {
                    std::ptr::read(game_bytes.as_ptr() as *const GameRecord)
                };
                
                // Extract result from flags
                let score = match game_record.result_and_flags & 0b11 {
                    0 => 0.0, // Loss
                    1 => 0.5, // Draw
                    2 => 1.0, // Win
                    _ => continue, // Invalid result
                };
                
                // Copy opponent_id_hash to avoid unaligned access to packed struct
                let opponent_id_hash = game_record.opponent_id_hash;
                
                // Look up opponent by hash in previous month's ratings
                let (opponent_rating, opponent_rd) = if let Some(opponent_id) = hash_to_player_id.get(&opponent_id_hash) {
                    if let Some(opponent_player) = previous_ratings.get(opponent_id) {
                        opponents_found += 1;
                        (opponent_player.rating, opponent_player.rd)
                    } else {
                        // This should not happen since we created the hash map from previous_ratings
                        warn!("Opponent ID {} found in hash map but not in previous ratings", opponent_id);
                        opponents_not_found += 1;
                        (BASE_RATING, BASE_RD)
                    }
                } else {
                    // Opponent not found in previous month's database
                    opponents_not_found += 1;
                    if opponents_not_found <= 100 { // Limit warnings to avoid spam
                        warn!("Opponent with hash {} not found in previous month's ratings database, using default rating {}", 
                              opponent_id_hash, BASE_RATING);
                    }
                    (BASE_RATING, BASE_RD)
                };
                
                let game_result = GameResult {
                    opponent_rating,
                    opponent_rd,
                    score,
                };
                
                player.games.push(game_result);
                total_games_loaded += 1;
            }
            
            // Log sample game for debugging
            if !player.games.is_empty() {
                let first_game = &player.games[0];
                let result_str = match first_game.score {
                    0.0 => "Loss",
                    0.5 => "Draw", 
                    1.0 => "Win",
                    _ => "Unknown"
                };
            }
            
        }
        
        info!("Opponent lookup stats: {} found, {} not found ({}% found)", 
              opponents_found, opponents_not_found, 
              if opponents_found + opponents_not_found > 0 { 
                  (opponents_found * 100) / (opponents_found + opponents_not_found) 
              } else { 
                  0 
              });
        
        if opponents_not_found > 100 {
            warn!("Total of {} opponents not found in previous month's database (showing only first 100 warnings)", opponents_not_found);
        }
        
        Ok(total_games_loaded)
    }

    async fn update_ratings_parallel(&self, players: &mut HashMap<String, Player>, time_control: &str) -> Result<ProcessingStats> {
        let total_players = players.len() as u32;
        let processed_count = Arc::new(AtomicU64::new(0));
        let total_games = Arc::new(AtomicU64::new(0));
        
        info!("Updating ratings for {} players using {} threads", total_players, self.workers);
        
        // Convert to vector for parallel processing
        let mut player_vec: Vec<_> = players.drain().collect();
        
        // Process players in parallel
        player_vec.par_iter_mut().for_each(|(_, player)| {
            let game_count = player.games.len() as u64;
            total_games.fetch_add(game_count, Ordering::Relaxed);
            
            self.update_player_rating(player);
            
            let processed = processed_count.fetch_add(1, Ordering::Relaxed) + 1;
            if processed % 5000 == 0 {
                info!("Processed {}/{} players", processed, total_players);
            }
        });
        
        // Put players back into HashMap
        *players = player_vec.into_iter().collect();
        
        // Apply new ratings
        for player in players.values_mut() {
            player.rating = player.new_rating;
            player.rd = player.new_rd;
            player.volatility = player.new_volatility;
            player.games.clear(); // Clear games for next month
        }
        
        let final_total_games = total_games.load(Ordering::Relaxed);
        
        Ok(ProcessingStats {
            total_players,
            processed_successfully: total_players,
            failed_players: 0,
            total_games: final_total_games,
            time_control: time_control.to_string(),
        })
    }

    fn update_player_rating(&self, player: &mut Player) {
        if player.games.is_empty() {
            // No games - increase RD due to time passage
            let phi = player.rd / SCALE;
            let phi_star = (phi * phi + player.volatility * player.volatility).sqrt();
            player.new_rd = (phi_star * SCALE).min(MAX_RD);
            player.new_rating = player.rating;
            player.new_volatility = player.volatility;
            return;
        }
        
        let mu = (player.rating - BASE_RATING) / SCALE;
        let phi = player.rd / SCALE;
        
        let mut v_inv = 0.0;
        let mut delta_sum = 0.0;
        
        for game in &player.games {
            let mu_j = (game.opponent_rating - BASE_RATING) / SCALE;
            let phi_j = game.opponent_rd / SCALE;
            
            let g_phi_j = 1.0 / (1.0 + (3.0 * phi_j * phi_j) / PI_SQUARED).sqrt();
            let e_val = 1.0 / (1.0 + (-g_phi_j * (mu - mu_j)).exp());
            
            v_inv += g_phi_j * g_phi_j * e_val * (1.0 - e_val);
            delta_sum += g_phi_j * (game.score - e_val);
        }
        
        let v = 1.0 / v_inv;
        let delta = v * delta_sum;
        
        let a  = (player.volatility * player.volatility).ln();   // ln σ²  (σ ≡ volatility)

        let f = |x: f64| {
            let exp_x = x.exp();
            (exp_x * (delta * delta - phi * phi - v - exp_x))
                / (2.0 * (phi * phi + v + exp_x).powi(2))
                - (x - a) / (TAU * TAU)
        };

        let mut A = a;
        let mut B;

        if delta * delta > phi * phi + v {
            // Initial upper bound per the paper
            B = (delta * delta - phi * phi - v).ln();
        } else {
            // Move downward until f(B) < 0
            let mut k = 1.0;
            loop {
                if f(a - k * TAU) >= 0.0 {
                    break;
                }
                k += 1.0;
            }
            B = a - k * TAU;
        }

        let mut f_a = f(A);
        let mut f_b = f(B);

        while (B - A).abs() > EPS {
            // Illinois variant of the secant method (used by Glickman)
            let C  = A + (A - B) * f_a / (f_b - f_a);
            let f_c = f(C);

            if f_c * f_b < 0.0 {
                A  = B;
                f_a = f_b;
            } else {
                f_a /= 2.0;     // halve to ensure convergence
            }
            B  = C;
            f_b = f_c;
        }

        let new_sigma = ((A + B) / 2.0 / 2.0).exp();   // σ′ = exp(x / 2) with x ≈ (A+B)/2
        let new_volatility = new_sigma.min(MAX_VOLATILITY);

        let phi_star = (phi * phi + new_volatility * new_volatility).sqrt();
        let new_phi  = 1.0 / (1.0 / (phi_star * phi_star) + 1.0 / v).sqrt();

        // Bounded rating change
        let rating_change   = new_phi * new_phi * delta_sum;
        let bounded_change  = rating_change
            .max(-700.0 / SCALE)
            .min( 700.0 / SCALE);

        let new_mu = mu + bounded_change;

        player.new_rating    = new_mu * SCALE + BASE_RATING;
        player.new_rd        = (new_phi * SCALE).min(MAX_RD);
        player.new_volatility = new_volatility;
    }

    async fn load_player_info(&self, time_control: &str) -> Result<HashMap<String, PlayerInfo>> {
        // Determine which player database to use based on period logic
        let database_month = self.get_player_database_month(&self.month_str)?;
        
        // For 2012 months 2-8, use standard database for rapid and blitz
        let database_time_control = if (time_control == "rapid" || time_control == "blitz") &&
                                        self.year == 2012 && self.month < 9 {
            "standard"
        } else {
            time_control
        };
        
        let db_s3_key = format!("persistent/player_info/processed/{}/{}.db", 
                               database_time_control, database_month);
        let local_db = self.temp_dir.join(format!("player_info_{}_{}.db", database_month, time_control));
        
        info!("Loading player info from database: {} (time_control: {}, database_month: {}, database_time_control: {})", 
              db_s3_key, time_control, database_month, database_time_control);
        
        if !self.check_s3_file_exists(&db_s3_key).await? {
            warn!("No player info database found for {}", time_control);
            return Ok(HashMap::new());
        }
        
        self.download_file(&db_s3_key, &local_db).await
            .context("Failed to download player info database")?;
        
        // Load player info from SQLite
        let player_info = tokio::task::spawn_blocking({
            let local_db = local_db.clone();
            move || -> Result<HashMap<String, PlayerInfo>> {
                let conn = Connection::open(&local_db)
                    .context("Failed to open player info database")?;
                
                let mut stmt = conn.prepare("SELECT id, name, fed, sex, b_year FROM players")
                    .context("Failed to prepare player info query")?;
                
                let mut player_info = HashMap::new();
                let mut total_records = 0;
                let mut empty_names = 0;
                
                let player_iter = stmt.query_map([], |row| {
                    total_records += 1;
                    
                    // Handle NULL birth_year gracefully
                    let birth_year_result: Result<u32, _> = row.get(4);
                    let birth_year = birth_year_result.ok(); // Convert error to None
                    
                    let name: String = row.get(1)?;
                    if name.is_empty() {
                        empty_names += 1;
                    }
                    
                    Ok(PlayerInfo {
                        id: row.get(0)?,
                        name,
                        federation: row.get(2)?,
                        sex: row.get(3)?,
                        birth_year,
                    })
                }).context("Failed to execute player info query")?;
                
                for player_result in player_iter {
                    let player = player_result.context("Failed to read player info row")?;
                    player_info.insert(player.id.clone(), player);
                }
                
                Ok(player_info)
            }
        }).await??;
        
        fs::remove_file(&local_db).await
            .context("Failed to remove temporary database file")?;
        
        info!("Loaded {} player info records", player_info.len());
        Ok(player_info)
    }

    fn get_player_database_month(&self, month_str: &str) -> Result<String> {
        let year: i32 = month_str[0..4].parse()?;
        let month: u32 = month_str[5..7].parse()?;
        
        let result = if year < 2009 || (year == 2009 && month < 9) {
            // 3-month periods
            match month {
                11 | 12 | 1 => format!("{}-01", year), 
                2 | 3 | 4 => format!("{}-04", year),    
                5 | 6 | 7 => format!("{}-07", year),   
                8 | 9 | 10 => format!("{}-10", year), 
                _ => month_str.to_string(),            
            }
        } else if year < 2012 || (year == 2012 && month < 8) {
            // 2-month periods (odd months are period-ending months)
            match month {
                12 | 1 => format!("{}-01", year),         
                2 | 3 => format!("{}-03", year),         
                4 | 5 => format!("{}-05", year),         
                6 | 7 => format!("{}-07", year),         
                8 | 9 => format!("{}-09", year),        
                10 | 11 => format!("{}-11", year),        
                _ => month_str.to_string(),
            }
        } else {
            // Monthly periods - use current month
            month_str.to_string()
        };
        
        Ok(result)
    }

    async fn save_ratings_parquet(&self, players: &HashMap<String, Player>, time_control: &str) -> Result<()> {
        info!("Saving {} player ratings to Parquet", players.len());
        
        let local_file = self.temp_dir.join(format!("{}_{}.parquet", self.month_str, time_control));
        
        // Prepare data for Parquet
        let mut player_ids = Vec::new();
        let mut ratings = Vec::new();
        let mut rds = Vec::new();
        let mut volatilities = Vec::new();
        
        for player in players.values() {
            player_ids.push(player.id.clone());
            ratings.push(player.rating);
            rds.push(player.rd);
            volatilities.push(player.volatility);
        }
        
        // Create Arrow schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("player_id", DataType::Utf8, false),
            Field::new("rating", DataType::Float64, false),
            Field::new("rd", DataType::Float64, false),
            Field::new("volatility", DataType::Float64, false),
        ]));
        
        // Create record batch
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(player_ids)),
                Arc::new(Float64Array::from(ratings)),
                Arc::new(Float64Array::from(rds)),
                Arc::new(Float64Array::from(volatilities)),
            ],
        ).context("Failed to create Arrow record batch")?;
        
        // Write Parquet file
        let file = std::fs::File::create(&local_file)
            .context("Failed to create local Parquet file")?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))
            .context("Failed to create Parquet writer")?;
        writer.write(&batch)
            .context("Failed to write record batch to Parquet")?;
        writer.close()
            .context("Failed to close Parquet writer")?;
        
        // Upload to S3
        let s3_key = format!("persistent/ratings/{}/{}.parquet", self.month_str, time_control);
        self.upload_file(&local_file, &s3_key).await
            .context("Failed to upload Parquet file to S3")?;
        
        fs::remove_file(&local_file).await
            .context("Failed to remove local Parquet file")?;
        
        Ok(())
    }

    async fn generate_top_rating_lists(&self, players: &HashMap<String, Player>, player_info: &HashMap<String, PlayerInfo>, time_control: &str) -> Result<()> {
        // Sort players by rating
        let mut sorted_players: Vec<_> = players.values()
            .filter(|p| p.rd <= 75.0) // Only include active players
            .collect();
        sorted_players.sort_by(|a, b| b.rating.partial_cmp(&a.rating).unwrap());
        
        info!("Generating top lists from {} active players (RD <= 75.0) out of {} total players", 
              sorted_players.len(), players.len());
        
        // Capture year for use in closures
        let year = self.year;
        
        // Generate different categories using boxed closures
        let categories: Vec<(&str, Box<dyn Fn(&Player, Option<&PlayerInfo>) -> bool + Send + Sync>)> = vec![
            ("open", Box::new(|_p: &Player, _info: Option<&PlayerInfo>| true)),
            ("women", Box::new(|_p: &Player, info: Option<&PlayerInfo>| {
                info.map_or(false, |i| i.sex == "F")
            })),
            ("juniors", Box::new(move |_p: &Player, info: Option<&PlayerInfo>| {
                info.and_then(|i| i.birth_year).map_or(false, |birth_year| year - birth_year as i32 <= 20)
            })),
            ("girls", Box::new(move |_p: &Player, info: Option<&PlayerInfo>| {
                info.map_or(false, |i| i.sex == "F" && 
                    i.birth_year.map_or(false, |birth_year| year - birth_year as i32 <= 20))
            })),
        ];
        
        for (category, filter_fn) in categories {
            let mut top_players = Vec::new();
            let mut rank = 0u32;
            let mut missing_info_count = 0;
            let mut empty_name_count = 0;
            
            for player in &sorted_players {
                let info = player_info.get(&player.id);
                
                if filter_fn(player, info) {
                    // Skip players with no info found
                    if info.is_none() {
                        continue;
                    }
                    
                    rank += 1;
                    
                    let entry = TopRatingEntry {
                        rank,
                        name: info.map_or_else(|| "".to_string(), |i| i.name.clone()),
                        federation: info.map_or_else(|| "".to_string(), |i| i.federation.clone()),
                        birth_year: info.and_then(|i| i.birth_year),
                        sex: if category == "women" || category == "girls" { 
                            None 
                        } else { 
                            info.map(|i| i.sex.clone()) 
                        },
                        rating: player.rating,
                        rd: player.rd,
                        player_id: player.id.clone(),
                    };
                    
                    // Log when we create an entry with missing player info
                    if info.is_none() {
                        warn!("Created top rating entry with missing info - rank: {}, player_id: \"{}\", rating: {:.6}, rd: {:.6}, name: \"\", federation: \"\", birth_year: null, sex: null", 
                              rank, player.id, player.rating, player.rd);
                    }
                    
                    top_players.push(entry);
                    
                    if rank >= 100 {
                        break;
                    }
                }
            }
            
            if !top_players.is_empty() {
                let local_file = self.temp_dir.join(format!("{}_{}.json", category, time_control));
                let json_data = serde_json::to_string_pretty(&top_players)
                    .context("Failed to serialize top players to JSON")?;
                fs::write(&local_file, json_data).await
                    .context("Failed to write JSON to local file")?;
                
                let s3_key = format!("persistent/top_ratings/{}/{}/{}.json", 
                                   self.month_str, time_control, category);
                self.upload_file(&local_file, &s3_key).await
                    .context("Failed to upload top ratings list to S3")?;
                
                fs::remove_file(&local_file).await
                    .context("Failed to remove local JSON file")?;
                
                info!("Generated {} category with {} players", category, top_players.len());
            }
        }
        
        Ok(())
    }

    // Utility methods with reduced logging
    fn calculate_previous_month(&self, month_str: &str) -> Result<String> {
        let year: i32 = month_str[0..4].parse()?;
        let month: u32 = month_str[5..7].parse()?;
        
        let (prev_year, prev_month) = if month == 1 {
            (year - 1, 12)
        } else {
            (year, month - 1)
        };
        
        Ok(format!("{:04}-{:02}", prev_year, prev_month))
    }

    async fn decompress_file(&self, compressed_path: &PathBuf, output_path: &PathBuf) -> Result<()> {
        use flate2::read::GzDecoder;
        
        let compressed_data = fs::read(compressed_path).await
            .context("Failed to read compressed file")?;
        
        let mut decoder = GzDecoder::new(&compressed_data[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)
            .context("Failed to decompress data")?;
        
        fs::write(output_path, decompressed).await
            .context("Failed to write decompressed data")?;
        
        Ok(())
    }

    async fn download_file(&self, s3_key: &str, local_path: &PathBuf) -> Result<()> {
        let response = self.s3_client
            .get_object()
            .bucket(&self.s3_bucket)
            .key(s3_key)
            .send()
            .await
            .context(format!("Failed to download file from S3: {}", s3_key))?;
        
        let data = response.body.collect().await
            .context("Failed to collect response body from S3")?
            .into_bytes();
        
        fs::write(local_path, data).await
            .context("Failed to write downloaded data to local file")?;
        
        Ok(())
    }

    async fn upload_file(&self, local_path: &PathBuf, s3_key: &str) -> Result<()> {
        let data = fs::read(local_path).await
            .context("Failed to read local file for upload")?;
        
        self.s3_client
            .put_object()
            .bucket(&self.s3_bucket)
            .key(s3_key)
            .body(data.into())
            .send()
            .await
            .context(format!("Failed to upload file to S3: {}", s3_key))?;
        
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
            Err(_) => Ok(false)
        }
    }

    async fn upload_processing_completion_marker(&self, results: &[(String, serde_json::Value)]) -> Result<()> {
        let completion_data = serde_json::json!({
            "month": self.month_str,
            "timestamp": Utc::now().to_rfc3339(),
            "step": "rating_processing",
            "input_format": "binary_packed",
            "output_format": "parquet_and_json",
            "results": results.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<HashMap<_, _>>()
        });
        
        let local_file = self.temp_dir.join(format!("rating_processing_completion_{}.json", self.month_str));
        
        fs::write(&local_file, serde_json::to_string_pretty(&completion_data)?)
            .await
            .context("Failed to write completion marker to local file")?;
        
        let s3_key = format!("results/{}/rating_processing_completion.json", self.month_str);
        
        self.upload_file(&local_file, &s3_key).await
            .context("Failed to upload completion marker to S3")?;
        
        fs::remove_file(&local_file).await
            .context("Failed to remove local completion marker file")?;
        
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(false)
        .with_level(true)
        .init();
    
    let args = Args::parse();
    
    // Validate argument combinations
    let months_to_process = match (&args.month, &args.first_month, &args.last_month) {
        (Some(month), None, None) => {
            // Single month mode
            validate_month_format(month)?;
            info!("Starting Rating Processor for single month: {}", month);
            vec![month.clone()]
        },
        (None, Some(first_month), Some(last_month)) => {
            // Range mode
            validate_month_format(first_month)?;
            validate_month_format(last_month)?;
            let months = generate_month_range(first_month, last_month)?;
            info!("Starting Rating Processor for {} months: {} to {}", months.len(), first_month, last_month);
            months
        },
        _ => {
            anyhow::bail!("Invalid argument combination. Use either --month for single month processing, or --first-month and --last-month for range processing.");
        }
    };
    
    info!("Processing {} month(s)", months_to_process.len());
    
    // Set up Rayon thread pool
    rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers)
        .build_global()
        .context("Failed to initialize thread pool")?;

    // Process each month in sequence
    for (index, month_str) in months_to_process.iter().enumerate() {
        info!("Processing month {}/{}: {}", index + 1, months_to_process.len(), month_str);
        
        let processor = RatingProcessor::new(
            args.s3_bucket.clone(),
            args.aws_region.clone(),
            month_str.clone(),
            args.workers,
        ).await?;
        
        match processor.process_ratings_for_month().await {
            Ok(()) => {
                info!("Rating processing completed successfully for {} ({}/{})", 
                      month_str, index + 1, months_to_process.len());
            }
            Err(e) => {
                error!("Processing failed for {} ({}/{}): {}", 
                       month_str, index + 1, months_to_process.len(), e);
                
                // Print the error chain for better debugging
                let mut current_error = e.source();
                let mut level = 1;
                while let Some(err) = current_error {
                    error!("  {}. Caused by: {}", level, err);
                    current_error = err.source();
                    level += 1;
                }
                
                std::process::exit(1);
            }
        }
    }
    
    info!("All {} month(s) processed successfully!", months_to_process.len());
    Ok(())
}