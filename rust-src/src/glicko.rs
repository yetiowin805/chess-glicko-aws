use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;
use aws_types::region::Region;
use chrono::{DateTime, Utc};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::fs;
use tracing::{error, info, warn};
use rayon::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use rusqlite::{Connection, Result as SqliteResult};

// Parquet/Arrow imports
use arrow::array::{Float64Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
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
const PI_SQUARED: f64 = std::f64::consts::PI * std::f64::consts::PI;

#[derive(Parser, Debug)]
#[command(name = "rating-processor")]
#[command(about = "Process binary game data and update Glicko-2 ratings")]
struct Args {
    #[arg(short, long, help = "Month for processing in YYYY-MM format")]
    month: String,
    
    #[arg(long, help = "S3 bucket for data storage")]
    s3_bucket: String,
    
    #[arg(long, default_value = "us-east-2", help = "AWS region")]
    aws_region: String,
    
    #[arg(long, default_value = "4", help = "Number of worker threads for parallel processing")]
    workers: usize,
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
    birth_year: u32,
}

#[derive(Debug, Serialize)]
struct TopRatingEntry {
    rank: u32,
    name: String,
    federation: String,
    birth_year: u32,
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
        let config = aws_config::from_env()
            .region(Region::new(aws_region))
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
        
        // Set up Rayon thread pool
        rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build_global()
            .context("Failed to initialize thread pool")?;
        
        info!("Initialized rating processor - S3 bucket: {}, workers: {}", s3_bucket, workers);
        
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
        
        info!("Processing time controls: {:?}", time_controls);
        
        // Process time controls in parallel
        let results: Vec<Result<ProcessingStats>> = time_controls
            .into_par_iter()
            .map(|time_control| {
                tokio::runtime::Handle::current().block_on(
                    self.process_time_control(time_control)
                )
            })
            .collect();
        
        let mut processing_results = Vec::new();
        for result in results {
            match result {
                Ok(stats) => {
                    info!("Successfully processed {}: {:?}", stats.time_control, stats);
                    processing_results.push((stats.time_control.clone(), serde_json::to_value(stats)?));
                }
                Err(e) => {
                    error!("Error processing time control: {}", e);
                    return Err(e);
                }
            }
        }
        
        self.upload_processing_completion_marker(&processing_results).await?;
        info!("Rating processing completed for {}", self.month_str);
        
        Ok(())
    }

    async fn process_time_control(&self, time_control: &str) -> Result<ProcessingStats> {
        info!("Processing time control: {}", time_control);
        
        // Check if already processed
        let parquet_s3_key = format!("persistent/ratings/{}/{}.parquet", self.month_str, time_control);
        if self.check_s3_file_exists(&parquet_s3_key).await? {
            info!("Ratings already processed for {}: {}", time_control, parquet_s3_key);
            return Ok(ProcessingStats {
                time_control: time_control.to_string(),
                ..Default::default()
            });
        }
        
        // Load current ratings from previous month
        let mut players = self.load_current_ratings(time_control).await?;
        info!("Loaded {} players with existing ratings", players.len());
        
        // Load and process games for this month
        let total_games_loaded = self.load_games_from_binary(time_control, &mut players).await?;
        info!("Loaded {} games for {} {}", total_games_loaded, time_control, self.month_str);
        
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
        
        Ok(stats)
    }

    async fn load_current_ratings(&self, time_control: &str) -> Result<HashMap<String, Player>> {
        // Try to load previous month's ratings
        let prev_month = self.calculate_previous_month(&self.month_str)?;
        let prev_ratings_key = format!("persistent/ratings/{}/{}.parquet", prev_month, time_control);
        
        if self.check_s3_file_exists(&prev_ratings_key).await? {
            info!("Loading ratings from previous month: {}", prev_month);
            return self.load_ratings_from_parquet(&prev_ratings_key).await;
        }
        
        // If no previous ratings, start with empty set
        info!("No previous ratings found, starting fresh for {}", time_control);
        Ok(HashMap::new())
    }

    async fn load_ratings_from_parquet(&self, s3_key: &str) -> Result<HashMap<String, Player>> {
        let local_file = self.temp_dir.join("previous_ratings.parquet");
        self.download_file(s3_key, &local_file).await?;
        
        let mut players = HashMap::new();
        
        // Read Parquet file using Arrow
        let file = std::fs::File::open(&local_file)?;
        let builder = parquet::arrow::ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;
        
        for batch_result in reader {
            let batch = batch_result?;
            let player_ids = batch.column(0).as_any().downcast_ref::<StringArray>()
                .context("Failed to read player_id column")?;
            let ratings = batch.column(1).as_any().downcast_ref::<Float64Array>()
                .context("Failed to read rating column")?;
            let rds = batch.column(2).as_any().downcast_ref::<Float64Array>()
                .context("Failed to read rd column")?;
            let volatilities = batch.column(3).as_any().downcast_ref::<Float64Array>()
                .context("Failed to read volatility column")?;
            
            for i in 0..batch.num_rows() {
                if let (Some(id), Some(rating), Some(rd), Some(volatility)) = (
                    player_ids.value(i).to_string(),
                    ratings.value(i),
                    rds.value(i), 
                    volatilities.value(i)
                ) {
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
        }
        
        fs::remove_file(&local_file).await?;
        info!("Loaded {} players from previous Parquet file", players.len());
        Ok(players)
    }

    async fn load_games_from_binary(&self, time_control: &str, players: &mut HashMap<String, Player>) -> Result<u64> {
        let binary_s3_key = format!("persistent/calculations_processed/{}_{}.bin.gz", self.month_str, time_control);
        
        if !self.check_s3_file_exists(&binary_s3_key).await? {
            info!("No games file found for {}: {}", time_control, binary_s3_key);
            return Ok(0);
        }
        
        let compressed_file = self.temp_dir.join(format!("{}_{}.bin.gz", self.month_str, time_control));
        let binary_file = self.temp_dir.join(format!("{}_{}.bin", self.month_str, time_control));
        
        // Download and decompress
        self.download_file(&binary_s3_key, &compressed_file).await?;
        self.decompress_file(&compressed_file, &binary_file).await?;
        
        // Read binary file
        let games_loaded = self.read_binary_games_file(&binary_file, players).await?;
        
        // Cleanup
        fs::remove_file(&compressed_file).await?;
        fs::remove_file(&binary_file).await?;
        
        Ok(games_loaded)
    }

    async fn read_binary_games_file(&self, file_path: &PathBuf, players: &mut HashMap<String, Player>) -> Result<u64> {
        let file = std::fs::File::open(file_path)?;
        let mut reader = BufReader::new(file);
        
        // Read file header
        let mut header_bytes = [0u8; std::mem::size_of::<FileHeader>()];
        reader.read_exact(&mut header_bytes)?;
        let header: FileHeader = unsafe { std::ptr::read(header_bytes.as_ptr() as *const FileHeader) };
        
        // Verify magic header and version
        if header.magic != *MAGIC_HEADER {
            anyhow::bail!("Invalid magic header in binary file");
        }
        if header.version != FORMAT_VERSION {
            anyhow::bail!("Unsupported file version: {}", header.version);
        }
        
        info!("Reading {} players with {} total games from {}", 
              header.player_count, header.total_games, file_path.display());
        
        let mut total_games_loaded = 0u64;
        
        // Read player data
        for _ in 0..header.player_count {
            // Read player header
            let mut player_header_bytes = [0u8; std::mem::size_of::<PlayerHeader>()];
            reader.read_exact(&mut player_header_bytes)?;
            let player_header: PlayerHeader = unsafe { 
                std::ptr::read(player_header_bytes.as_ptr() as *const PlayerHeader) 
            };
            
            // Read player ID
            let mut player_id_bytes = vec![0u8; player_header.player_id_len as usize];
            reader.read_exact(&mut player_id_bytes)?;
            let player_id = String::from_utf8(player_id_bytes)?;
            
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
            for _ in 0..player_header.game_count {
                let mut game_bytes = [0u8; std::mem::size_of::<GameRecord>()];
                reader.read_exact(&mut game_bytes)?;
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
                
                // Use opponent rating if available, otherwise use default
                let opponent_rating = if game_record.opponent_rating > 0 {
                    game_record.opponent_rating as f64
                } else {
                    BASE_RATING
                };
                
                let game_result = GameResult {
                    opponent_rating,
                    opponent_rd: BASE_RD, // Default RD for opponents
                    score,
                };
                
                player.games.push(game_result);
                total_games_loaded += 1;
            }
        }
        
        info!("Loaded {} games from binary file", total_games_loaded);
        Ok(total_games_loaded)
    }

    async fn update_ratings_parallel(&self, players: &mut HashMap<String, Player>, time_control: &str) -> Result<ProcessingStats> {
        let total_players = players.len() as u32;
        let processed_count = Arc::new(AtomicU64::new(0));
        let total_games = Arc::new(AtomicU64::new(0));
        
        info!("Updating ratings for {} players in parallel", total_players);
        
        // Convert to vector for parallel processing
        let mut player_vec: Vec<_> = players.drain().collect();
        
        // Process players in parallel
        player_vec.par_iter_mut().for_each(|(_, player)| {
            let game_count = player.games.len() as u64;
            total_games.fetch_add(game_count, Ordering::Relaxed);
            
            self.update_player_rating(player);
            
            let processed = processed_count.fetch_add(1, Ordering::Relaxed) + 1;
            if processed % 1000 == 0 {
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
        info!("Completed rating updates: {} players, {} games", total_players, final_total_games);
        
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
        
        // Simplified volatility update
        let a = (player.volatility * player.volatility).ln();
        let new_volatility = if delta * delta > phi * phi + v {
            ((delta * delta - phi * phi - v).ln() / 2.0).exp()
        } else {
            player.volatility
        }.min(MAX_VOLATILITY);
        
        let phi_star = (phi * phi + new_volatility * new_volatility).sqrt();
        let new_phi = 1.0 / (1.0 / (phi_star * phi_star) + 1.0 / v).sqrt();
        
        // Rating update with bounds
        let rating_change = new_phi * new_phi * delta_sum;
        let bounded_change = rating_change.max(-1000.0 / SCALE).min(1000.0 / SCALE);
        let new_mu = mu + bounded_change;
        
        player.new_rating = new_mu * SCALE + BASE_RATING;
        player.new_rd = (new_phi * SCALE).min(MAX_RD);
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
        
        if !self.check_s3_file_exists(&db_s3_key).await? {
            warn!("No player info database found for {} (using {})", time_control, database_month);
            return Ok(HashMap::new());
        }
        
        self.download_file(&db_s3_key, &local_db).await?;
        
        // Load player info from SQLite
        let player_info = tokio::task::spawn_blocking({
            let local_db = local_db.clone();
            move || -> Result<HashMap<String, PlayerInfo>> {
                let conn = Connection::open(&local_db)
                    .context("Failed to open player info database")?;
                
                let mut stmt = conn.prepare("SELECT id, name, federation, sex, birth_year FROM players")
                    .context("Failed to prepare player info query")?;
                
                let mut player_info = HashMap::new();
                let player_iter = stmt.query_map([], |row| {
                    Ok(PlayerInfo {
                        id: row.get(0)?,
                        name: row.get(1)?,
                        federation: row.get(2)?,
                        sex: row.get(3)?,
                        birth_year: row.get(4)?,
                    })
                }).context("Failed to execute player info query")?;
                
                for player_result in player_iter {
                    let player = player_result.context("Failed to read player info row")?;
                    player_info.insert(player.id.clone(), player);
                }
                
                info!("Loaded {} player info records", player_info.len());
                Ok(player_info)
            }
        }).await??;
        
        fs::remove_file(&local_db).await?;
        Ok(player_info)
    }

    fn get_player_database_month(&self, month_str: &str) -> Result<String> {
        let year: i32 = month_str[0..4].parse()?;
        let month: u32 = month_str[5..7].parse()?;
        
        // Player database selection: earlier months in a period use the previous period's 
        // final database, while the last month of a period uses its own database
        if year < 2009 || (year == 2009 && month < 9) {
            // 3-month periods
            match month {
                1 | 2 | 3 => Ok(format!("{}-01", year)),      // Feb/Mar use Jan database

                4 | 5 | 6 => Ok(format!("{}-04", year)),      // May/Jun use Apr database

                7 | 8 | 9 => Ok(format!("{}-07", year)),      // Aug/Sep use Jul database

                10 | 11 | 12 => Ok(format!("{}-10", year)),         // Oct uses Oct database
                
                _ => Ok(month_str.to_string()),           // Non-period month
            }
        } else if year < 2012 || (year == 2012 && month < 8) {
            // 2-month periods (odd months are period-ending months)
            match month {
                1 | 2 => Ok(format!("{}-01", year)),          // Jan uses Jan database
                
                3 | 4 => Ok(format!("{}-03", year)),          // Mar uses Mar database
                
                5 | 6 => Ok(format!("{}-05", year)),          // May uses May database
                
                7 | 8 => Ok(format!("{}-07", year)),          // Jul uses Jul database
                    
                9 | 10 => Ok(format!("{}-09", year)),          // Sep uses Sep database
                
                11 | 12 => Ok(format!("{}-11", year)),         // Nov uses Nov database
                
                _ => Ok(month_str.to_string()),
            }
        } else {
            // Monthly periods - use current month
            Ok(month_str.to_string())
        }
    }

    async fn save_ratings_parquet(&self, players: &HashMap<String, Player>, time_control: &str) -> Result<()> {
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
        )?;
        
        // Write Parquet file
        let file = std::fs::File::create(&local_file)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
        
        // Upload to S3
        let s3_key = format!("persistent/ratings/{}/{}.parquet", self.month_str, time_control);
        self.upload_file(&local_file, &s3_key).await?;
        
        fs::remove_file(&local_file).await?;
        info!("Saved {} player ratings to {}", players.len(), s3_key);
        
        Ok(())
    }

    async fn generate_top_rating_lists(&self, players: &HashMap<String, Player>, player_info: &HashMap<String, PlayerInfo>, time_control: &str) -> Result<()> {
        // Sort players by rating
        let mut sorted_players: Vec<_> = players.values()
            .filter(|p| p.rd <= 75.0) // Only include active players
            .collect();
        sorted_players.sort_by(|a, b| b.rating.partial_cmp(&a.rating).unwrap());
        
        // Generate different categories
        let categories = vec![
            ("open", |_p: &Player, _info: Option<&PlayerInfo>| true),
            ("women", |_p: &Player, info: Option<&PlayerInfo>| {
                info.map_or(false, |i| i.sex == "F")
            }),
            ("juniors", |_p: &Player, info: Option<&PlayerInfo>| {
                info.map_or(false, |i| self.year - i.birth_year as i32 <= 20)
            }),
            ("girls", |_p: &Player, info: Option<&PlayerInfo>| {
                info.map_or(false, |i| i.sex == "F" && self.year - i.birth_year as i32 <= 20)
            }),
        ];
        
        for (category, filter_fn) in categories {
            let mut top_players = Vec::new();
            let mut rank = 0u32;
            
            for player in &sorted_players {
                let info = player_info.get(&player.id);
                
                if filter_fn(player, info) {
                    rank += 1;
                    
                    let entry = TopRatingEntry {
                        rank,
                        name: info.map_or_else(|| "".to_string(), |i| i.name.clone()),
                        federation: info.map_or_else(|| "".to_string(), |i| i.federation.clone()),
                        birth_year: info.map_or(0, |i| i.birth_year),
                        sex: if category == "women" || category == "girls" { 
                            None 
                        } else { 
                            info.map(|i| i.sex.clone()) 
                        },
                        rating: player.rating,
                        rd: player.rd,
                        player_id: player.id.clone(),
                    };
                    
                    top_players.push(entry);
                    
                    if rank >= 100 {
                        break;
                    }
                }
            }
            
            if !top_players.is_empty() {
                let local_file = self.temp_dir.join(format!("{}_{}.json", category, time_control));
                let json_data = serde_json::to_string_pretty(&top_players)?;
                fs::write(&local_file, json_data).await?;
                
                let s3_key = format!("persistent/top_ratings/{}/{}/{}.json", 
                                   self.month_str, time_control, category);
                self.upload_file(&local_file, &s3_key).await?;
                
                fs::remove_file(&local_file).await?;
                info!("Generated top {} list for {} with {} players", category, time_control, top_players.len());
            }
        }
        
        Ok(())
    }

    // Utility methods
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
        
        let compressed_data = fs::read(compressed_path).await?;
        let mut decoder = GzDecoder::new(&compressed_data[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        fs::write(output_path, decompressed).await?;
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
            "step": "rating_processing",
            "input_format": "binary_packed",
            "output_format": "parquet_and_json",
            "results": results.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<HashMap<_, _>>()
        });
        
        let local_file = self.temp_dir.join(format!("rating_processing_completion_{}.json", self.month_str));
        fs::write(&local_file, serde_json::to_string_pretty(&completion_data)?).await?;
        
        let s3_key = format!("results/{}/rating_processing_completion.json", self.month_str);
        self.upload_file(&local_file, &s3_key).await?;
        
        fs::remove_file(&local_file).await?;
        info!("Uploaded processing completion marker for {}", self.month_str);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    
    let args = Args::parse();
    
    // Validate month format
    chrono::NaiveDate::parse_from_str(&format!("{}-01", args.month), "%Y-%m-%d")
        .context("Month must be in YYYY-MM format")?;
    
    let processor = RatingProcessor::new(
        args.s3_bucket,
        args.aws_region,
        args.month,
        args.workers,
    ).await?;
    
    match processor.process_ratings_for_month().await {
        Ok(()) => {
            info!("Rating processing completed successfully");
            Ok(())
        }
        Err(e) => {
            error!("Processing failed with error: {}", e);
            std::process::exit(1);
        }
    }
}