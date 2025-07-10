use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;
use aws_types::region::Region;
use chrono::{DateTime, Utc};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use tokio::fs;
use tracing::{error, info, warn};
use regex::Regex;
use rusqlite::{Connection, Result as SqliteResult};

// Binary format constants
const MAGIC_HEADER: &[u8; 8] = b"CHSSGAME";
const FORMAT_VERSION: u16 = 1;

// Time controls
const TIME_CONTROLS: &[&str] = &["standard", "rapid", "blitz"];

// Helper function for normalizing player names (used in blocking context)
fn normalize_player_name(name: &str) -> String {
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

#[derive(Parser, Debug)]
#[command(name = "calculation-processor")]
#[command(about = "Process FIDE calculation data from consolidated JSONL format with multi-month support")]
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
#[derive(Debug, Default, Serialize)]
struct ProcessingStats {
    total_players: u32,
    processed_successfully: u32,
    failed_players: u32,
    total_games: u64,
}

// Player mapping structures
struct PlayerMappings {
    exact_mappings: HashMap<String, String>,
    normalized_mappings: HashMap<String, String>,
}

// Tournament mapping cache
struct TournamentMappings {
    tournament_to_month: HashMap<String, String>, // tournament_id -> target_month
}

// Multi-month processed data
#[derive(Debug)]
struct MultiMonthProcessedData {
    data_by_month: HashMap<String, Vec<ProcessedPlayer>>, // month -> players
}

struct CalculationProcessor {
    s3_client: S3Client,
    s3_bucket: String,
    temp_dir: PathBuf,
    month_str: String,
    year: i32,
    month: u32,
    period_months: Vec<String>,
    is_multi_month: bool,
}

impl CalculationProcessor {
    async fn new(s3_bucket: String, aws_region: String, month_str: String) -> Result<Self> {
        let config = aws_config::from_env()
            .region(Region::new(aws_region.clone()))
            .load()
            .await;
        
        let s3_client = S3Client::new(&config);
        let temp_dir = PathBuf::from("/tmp/chess_data");
        
        // Create temp directory
        fs::create_dir_all(&temp_dir).await
            .context("Failed to create temp directory")?;
        
        // Parse year and month
        let year: i32 = month_str[0..4].parse()
            .context("Invalid month format")?;
        let month: u32 = month_str[5..7].parse()
            .context("Invalid month format")?;
        
        // Determine period months and multi-month status
        let (period_months, is_multi_month) = Self::get_period_months(year, month);
        
        info!("Initialized calculation processor - S3 bucket: {}, temp dir: {:?}", 
              s3_bucket, temp_dir);
        info!("Period months: {:?}, Multi-month: {}", period_months, is_multi_month);
        
        Ok(Self {
            s3_client,
            s3_bucket,
            temp_dir,
            month_str,
            year,
            month,
            period_months,
            is_multi_month,
        })
    }

    fn get_period_months(year: i32, month: u32) -> (Vec<String>, bool) {
        if year < 2009 || (year == 2009 && month < 9) {
            // 3-month periods
            let months = match month {
                1 => vec![format!("{}-11", year - 1), format!("{}-12", year - 1), format!("{}-01", year)],
                4 => vec![format!("{}-02", year), format!("{}-03", year), format!("{}-04", year)],
                7 => vec![format!("{}-05", year), format!("{}-06", year), format!("{}-07", year)],
                10 => vec![format!("{}-08", year), format!("{}-09", year), format!("{}-10", year)],
                _ => vec![format!("{}-{:02}", year, month)],
            };
            (months, months.len() > 1)
        } else if year < 2012 || (year == 2012 && month < 8) {
            // 2-month periods (odd months)
            if month % 2 == 1 {
                let months = match month {
                    1 => vec![format!("{}-12", year - 1), format!("{}-01", year)],
                    3 => vec![format!("{}-02", year), format!("{}-03", year)],
                    5 => vec![format!("{}-04", year), format!("{}-05", year)],
                    7 => vec![format!("{}-06", year), format!("{}-07", year)],
                    _ => vec![format!("{}-{:02}", year, month)],
                };
                (months, months.len() > 1)
            } else {
                (vec![format!("{}-{:02}", year, month)], false)
            }
        } else {
            // Monthly periods
            (vec![format!("{}-{:02}", year, month)], false)
        }
    }

    async fn process_calculations_for_month(&self) -> Result<bool> {
        info!("Starting calculation processing for {} (Multi-month: {})", 
              self.month_str, self.is_multi_month);
        
        // Determine which time controls to process
        let time_controls_to_process = if self.year > 2012 || (self.year == 2012 && self.month >= 2) {
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
        // Check if already processed (for multi-month, check all months)
        if self.is_multi_month {
            let mut all_exist = true;
            for month in &self.period_months {
                let output_s3_key = format!("persistent/calculations_processed/{}.bin", 
                                           self.get_output_filename(time_control, month));
                if !self.check_s3_file_exists(&output_s3_key).await? {
                    all_exist = false;
                    break;
                }
            }
            if all_exist {
                info!("Processed calculations already exist for {} (all months): {:?}", 
                      time_control, self.period_months);
                return Ok(ProcessingStats::default());
            }
        } else {
            let output_s3_key = format!("persistent/calculations_processed/{}.bin", 
                                       self.get_output_filename(time_control, &self.month_str));
            if self.check_s3_file_exists(&output_s3_key).await? {
                info!("Processed calculations already exist for {}: {}", time_control, output_s3_key);
                return Ok(ProcessingStats::default());
            }
        }
        
        // Download JSONL file
        let jsonl_s3_key = format!("persistent/calculations/{}/{}.jsonl", 
                                  self.month_str, time_control);
        
        if !self.check_s3_file_exists(&jsonl_s3_key).await? {
            info!("No calculation file found for {}: {}", time_control, jsonl_s3_key);
            return Ok(ProcessingStats::default());
        }
        
        info!("Found calculation file for {}: {}", time_control, jsonl_s3_key);
        
        // Load all required data into memory
        let player_mappings = self.load_player_database(time_control).await?;
        let tournament_mappings = if self.is_multi_month {
            Some(self.load_tournament_databases(time_control).await?)
        } else {
            None
        };
        
        // Download and process JSONL file
        let local_jsonl_path = self.temp_dir.join(format!("{}.jsonl", time_control));
        self.download_file(&jsonl_s3_key, &local_jsonl_path).await?;
        
        let (processed_data, stats) = self.process_jsonl_file(
            &local_jsonl_path, 
            &player_mappings, 
            tournament_mappings.as_ref()
        ).await?;
        
        // Clean up JSONL file
        fs::remove_file(&local_jsonl_path).await?;
        
        // Save processed data
        if self.is_multi_month {
            self.save_multi_month_processed_data(&processed_data, time_control, time_control_index, &stats).await?;
        } else {
            if let Some(single_month_data) = processed_data.data_by_month.get(&self.month_str) {
                if !single_month_data.is_empty() {
                    self.save_processed_calculations_binary(
                        single_month_data, 
                        time_control, 
                        time_control_index, 
                        &stats,
                        &self.month_str
                    ).await?;
                }
            }
        }
        
        info!("{}: Processing completed - {:?}", time_control, stats);
        Ok(stats)
    }

    async fn load_tournament_databases(&self, time_control: &str) -> Result<TournamentMappings> {
        info!("Loading tournament databases for {}", time_control);
        
        let mut tournament_to_month = HashMap::new();
        
        // Load tournament database for this time control
        let s3_key = format!("persistent/tournament_data/processed/{}/{}.db", 
                            time_control, self.month_str);
        let local_db_path = self.temp_dir.join(format!("tournaments_{}_{}.db", time_control, self.month_str));
        
        self.download_file(&s3_key, &local_db_path).await
            .context("Failed to download tournament database")?;
        
        // Load tournament mappings from SQLite database
        let mappings = tokio::task::spawn_blocking({
            let local_db_path = local_db_path.clone();
            move || -> Result<HashMap<String, String>> {
                let conn = Connection::open(&local_db_path)
                    .context("Failed to open tournament database")?;
                
                let mut stmt = conn.prepare("SELECT tournament_id, target_month FROM tournament_players")
                    .context("Failed to prepare tournament query")?;
                
                let mut mappings = HashMap::new();
                let tournament_iter = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?, // tournament_id
                        row.get::<_, String>(1)?, // target_month
                    ))
                }).context("Failed to execute tournament query")?;
                
                for tournament_result in tournament_iter {
                    let (tournament_id, target_month) = tournament_result
                        .context("Failed to read tournament row")?;
                    mappings.insert(tournament_id, target_month);
                }
                
                info!("Loaded {} tournament mappings", mappings.len());
                Ok(mappings)
            }
        }).await??;
        
        tournament_to_month.extend(mappings);
        
        // Clean up database file
        fs::remove_file(&local_db_path).await?;
        
        Ok(TournamentMappings {
            tournament_to_month,
        })
    }

    async fn load_player_database(&self, time_control: &str) -> Result<PlayerMappings> {
        // For 2012 months 2-8, use standard rating lists for rapid and blitz
        let database_time_control = if (time_control == "rapid" || time_control == "blitz") &&
                                        self.year == 2012 && self.month < 9 {
            "standard"
        } else {
            time_control
        };
        
        let s3_key = format!("persistent/player_info/processed/{}/{}.db", 
                            database_time_control, self.month_str);
        let local_db_path = self.temp_dir.join(format!("{}_{}.db", self.month_str, time_control));
        
        info!("Downloading player database for {} (using {} database)...", time_control, database_time_control);
        
        self.download_file(&s3_key, &local_db_path).await
            .context("Failed to download player database")?;
        
        // Load player mappings from SQLite database
        let player_mappings = tokio::task::spawn_blocking({
            let local_db_path = local_db_path.clone();
            let time_control = time_control.to_string();
            move || -> Result<PlayerMappings> {
                let conn = Connection::open(&local_db_path)
                    .context("Failed to open SQLite database")?;
                
                let player_count: i64 = conn.query_row(
                    "SELECT COUNT(*) FROM players",
                    [],
                    |row| row.get(0)
                ).context("Failed to query player count")?;
                
                info!("Loading {} players from database for {}", player_count, time_control);
                
                let mut exact_mappings = HashMap::new();
                let mut normalized_mappings = HashMap::new();
                
                let mut stmt = conn.prepare("SELECT id, name FROM players")
                    .context("Failed to prepare player query")?;
                
                let player_iter = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?, // id
                        row.get::<_, String>(1)?, // name
                    ))
                }).context("Failed to execute player query")?;
                
                for player_result in player_iter {
                    let (player_id, name) = player_result
                        .context("Failed to read player row")?;
                    
                    exact_mappings.insert(name.to_lowercase(), player_id.clone());
                    
                    let normalized = normalize_player_name(&name);
                    if !normalized.is_empty() {
                        normalized_mappings.insert(normalized, player_id);
                    }
                }
                
                info!("Pre-loaded {} exact mappings and {} normalized mappings for {}", 
                      exact_mappings.len(), normalized_mappings.len(), time_control);
                
                Ok(PlayerMappings {
                    exact_mappings,
                    normalized_mappings,
                })
            }
        }).await??;
        
        // Clean up database file
        fs::remove_file(&local_db_path).await?;
        
        Ok(player_mappings)
    }

    async fn process_jsonl_file(
        &self, 
        jsonl_path: &PathBuf, 
        player_mappings: &PlayerMappings,
        tournament_mappings: Option<&TournamentMappings>
    ) -> Result<(MultiMonthProcessedData, ProcessingStats)> {
        let mut processed_data = MultiMonthProcessedData {
            data_by_month: HashMap::new(),
        };
        
        // Initialize data structures for each month
        for month in &self.period_months {
            processed_data.data_by_month.insert(month.clone(), Vec::new());
        }
        
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
                    match self.process_single_calculation(&player_record, player_mappings, tournament_mappings).await {
                        Ok(month_players) => {
                            let mut any_games = false;
                            for (month, processed_player_opt) in month_players {
                                if let Some(processed_player) = processed_player_opt {
                                    stats.total_games += processed_player.games.len() as u64;
                                    if let Some(month_data) = processed_data.data_by_month.get_mut(&month) {
                                        month_data.push(processed_player);
                                    }
                                    any_games = true;
                                }
                            }
                            if any_games {
                                stats.processed_successfully += 1;
                            } else {
                                stats.failed_players += 1;
                            }
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

    async fn process_single_calculation(
        &self, 
        player_record: &PlayerRecord, 
        player_mappings: &PlayerMappings,
        tournament_mappings: Option<&TournamentMappings>
    ) -> Result<HashMap<String, Option<ProcessedPlayer>>> {
        let mut games_by_month: HashMap<String, Vec<ProcessedGame>> = HashMap::new();
        
        // Initialize games for each month
        for month in &self.period_months {
            games_by_month.insert(month.clone(), Vec::new());
        }
        
        for tournament in &player_record.calculation_data.tournaments {
            let tournament_id_hash = self.hash_string(&tournament.tournament_id);
            let is_unrated = tournament.player_is_unrated.unwrap_or(false);
            
            // Determine target month for this tournament
            let target_month = if let Some(mappings) = tournament_mappings {
                mappings.tournament_to_month.get(&tournament.tournament_id)
                    .cloned()
                    .unwrap_or_else(|| {
                        warn!("Tournament {} not found in mappings, using default month", tournament.tournament_id);
                        self.period_months.last().unwrap().clone()
                    })
            } else {
                self.month_str.clone()
            };
            
            // Skip if target month is not in our period
            // This shouldn't happen
            if !self.period_months.contains(&target_month) {
                return Err(anyhow::anyhow!(
                    "Tournament {} allocated to month {} which is not in period {:?}",
                    tournament.tournament_id, target_month, self.period_months
                ));
            }
            
            for game in &tournament.games {
                let opponent_name = game.opponent_name.trim();
                if opponent_name.is_empty() {
                    continue;
                }
                
                let score = self.convert_result_to_score(&game.result)?;
                let opponent_id = self.get_player_id_from_name(opponent_name, player_mappings);
                
                let opponent_id_hash = match opponent_id {
                    Some(id) => self.hash_string(&id),
                    None => {
                        warn!("Could not resolve opponent ID for '{}' in tournament {}, player {}", 
                              opponent_name, tournament.tournament_id, player_record.player_id);
                        continue;
                    }
                };
                
                let opponent_rating = game.opponent_rating
                    .as_ref()
                    .and_then(|r| r.parse::<u16>().ok())
                    .unwrap_or(0);
                
                let mut result_and_flags = score;
                if is_unrated {
                    result_and_flags |= 0b100;
                }
                
                let game_record = ProcessedGame {
                    tournament_id_hash,
                    opponent_id_hash,
                    opponent_rating,
                    result_and_flags,
                };
                
                if let Some(month_games) = games_by_month.get_mut(&target_month) {
                    month_games.push(game_record);
                }
            }
        }
        
        // Create processed players for each month
        let mut result = HashMap::new();
        for (month, games) in games_by_month {
            if games.is_empty() {
                result.insert(month, None);
            } else {
                result.insert(month, Some(ProcessedPlayer {
                    player_id: player_record.player_id.clone(),
                    games,
                }));
            }
        }
        
        Ok(result)
    }

    async fn save_multi_month_processed_data(
        &self, 
        processed_data: &MultiMonthProcessedData, 
        time_control: &str, 
        time_control_index: u8,
        stats: &ProcessingStats
    ) -> Result<()> {
        for (month, players) in &processed_data.data_by_month {
            if !players.is_empty() {
                let month_stats = ProcessingStats {
                    total_players: players.len() as u32,
                    processed_successfully: players.len() as u32,
                    failed_players: 0,
                    total_games: players.iter().map(|p| p.games.len() as u64).sum(),
                };
                
                self.save_processed_calculations_binary(
                    players, 
                    time_control, 
                    time_control_index, 
                    &month_stats,
                    month
                ).await?;
            }
        }
        Ok(())
    }

    fn convert_result_to_score(&self, result: &str) -> Result<u8> {
        match result.trim() {
            "1" | "1.0" | "1.00" => Ok(2), // Win
            "0.5" | "0.50" => Ok(1),       // Draw
            "0" | "0.0" | "0.00" => Ok(0), // Loss
            _ => anyhow::bail!("Unknown result format: {}", result),
        }
    }

    fn get_player_id_from_name(&self, name: &str, player_mappings: &PlayerMappings) -> Option<String> {
        if name.is_empty() {
            return None;
        }
        
        if let Some(id) = player_mappings.exact_mappings.get(&name.to_lowercase()) {
            return Some(id.clone());
        }
        
        let normalized_name = normalize_player_name(name);
        if !normalized_name.is_empty() {
            if let Some(id) = player_mappings.normalized_mappings.get(&normalized_name) {
                return Some(id.clone());
            }
        }
        
        None
    }

    fn hash_string(&self, s: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        let mut hasher = DefaultHasher::new();
        s.hash(&mut hasher);
        hasher.finish()
    }

    async fn save_processed_calculations_binary(
        &self, 
        processed_data: &[ProcessedPlayer], 
        time_control: &str, 
        time_control_index: u8, 
        stats: &ProcessingStats,
        target_month: &str
    ) -> Result<()> {
        let output_filename = self.get_output_filename(time_control, target_month);
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
            
            file.write_all(player.player_id.as_bytes())?;
            
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
        
        info!("Saved {} processed calculations to {} for month {}", 
              processed_data.len(), s3_key, target_month);
        Ok(())
    }

    fn get_output_filename(&self, time_control: &str, target_month: &str) -> String {
        format!("{}_{}.bin", target_month, time_control)
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
            "period_months": self.period_months,
            "is_multi_month": self.is_multi_month,
            "timestamp": Utc::now().to_rfc3339(),
            "step": "calculation_processing_multi_month",
            "input_format": "consolidated_jsonl",
            "output_format": "binary_packed",
            "results": results.iter().map(|(k, v)| (k.clone(), v.clone())).collect::<HashMap<_, _>>()
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