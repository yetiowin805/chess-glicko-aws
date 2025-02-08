import os
import sys
import math
import boto3
import logging
import tempfile
import pandas as pd
import shutil
from countries import countries
from botocore.exceptions import ClientError

PLAYER_INFO_BUCKET = "chess-glicko-player-info"
GAMES_BUCKET = "chess-glicko-clean-games"
OUTPUT_BUCKET = "chess-glicko-rating-lists"

PLAYER_INFO_KEY_TEMPLATE = "processed/{year:04d}-{month:02d}.txt"

GAMES_KEY_TEMPLATE = "tournament-data/{year:04d}_{month:02d}/games_{time_control_lower}.csv"
PREV_RATINGS_KEY_TEMPLATE = "{time_control}/{year:04d}-{month:02d}.txt"
OUTPUT_RATINGS_KEY_TEMPLATE = "{time_control}/{year:04d}-{month:02d}.txt"

RATING_CONSTANTS = {
    "BASE_RATING": 1500.0,
    "BASE_RD": 350.0,
    "BASE_VOLATILITY": 0.09,
    "TAU": 0.2,
    "SCALE": 173.7178,
    "MAX_RD": 500,
    "MAX_VOLATILITY": 0.1,
}

MATH_CONSTANTS = {"PI_SQUARED": math.pi ** 2}
FEDERATIONS = countries

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

s3_client = boto3.client('s3')

class GameResult:
    """Represents the result of a game between two players."""
    def __init__(self, opponent_id=None, score=None, opponent_rating=None, opponent_rd=None):
        if opponent_id is not None and score is not None:
            self.opponent_id = opponent_id
            self.score = score
            self.generatedGame = False
        elif opponent_rating is not None and opponent_rd is not None and score is not None:
            self.opponent_rating = opponent_rating
            self.opponent_rd = opponent_rd
            self.score = score
            self.generatedGame = True
        else:
            raise ValueError("Invalid arguments for GameResult")

class Player:
    """Represents a player with Glicko-2 rating attributes."""
    def __init__(self, player_id, rating=RATING_CONSTANTS["BASE_RATING"],
                 rd=RATING_CONSTANTS["BASE_RD"],
                 volatility=RATING_CONSTANTS["BASE_VOLATILITY"]):
        self.id = player_id
        self.rating = rating
        self.rd = rd
        self.volatility = volatility
        self.new_rating = rating
        self.new_rd = rd
        self.games = []

def get_previous_month(year, month):
    return (year - 1, 12) if month == 1 else (year, month - 1)

def add_game(player_id, game, players_dict):
    player = players_dict.setdefault(player_id, Player(player_id))
    if not game.generatedGame:
        players_dict.setdefault(game.opponent_id, Player(game.opponent_id))
    player.games.append(game)

def f(x, delta, v, A):
    ex = math.exp(x)
    return (ex * (delta**2 - v - ex)) / (2 * (v + ex)**2) - (x - A) / RATING_CONSTANTS["TAU"]**2

def glicko2_update(target, players):
    if not target.games:
        phi = target.rd / RATING_CONSTANTS["SCALE"]
        target.new_rd = min(math.sqrt(phi**2 + target.volatility**2) * RATING_CONSTANTS["SCALE"], RATING_CONSTANTS["MAX_RD"])
        return

    mu = (target.rating - RATING_CONSTANTS["BASE_RATING"]) / RATING_CONSTANTS["SCALE"]
    phi = target.rd / RATING_CONSTANTS["SCALE"]

    v_inv = 0
    delta_sum = 0
    for game in target.games:
        if not game.generatedGame:
            opponent = players[game.opponent_id]
            mu_j = (opponent.rating - RATING_CONSTANTS["BASE_RATING"]) / RATING_CONSTANTS["SCALE"]
            phi_j = opponent.rd / RATING_CONSTANTS["SCALE"]
        else:
            mu_j = (game.opponent_rating - RATING_CONSTANTS["BASE_RATING"]) / RATING_CONSTANTS["SCALE"]
            phi_j = game.opponent_rd / RATING_CONSTANTS["SCALE"]
        g_phi_j = 1.0 / math.sqrt(1.0 + (3.0 * phi_j**2) / MATH_CONSTANTS["PI_SQUARED"])
        e_val = 1.0 / (1.0 + math.exp(-g_phi_j * (mu - mu_j)))
        v_inv += g_phi_j**2 * e_val * (1 - e_val)
        delta_sum += g_phi_j * (game.score - e_val)
    v = 1.0 / v_inv
    delta = v * delta_sum

    a = math.log(target.volatility**2)
    A = a
    if delta**2 > phi**2 + v:
        B = math.log(delta**2 - phi**2 - v)
    else:
        k = 1
        while f(a - k * RATING_CONSTANTS["TAU"], delta, v, a) < 0:
            k += 1
        B = a - k * RATING_CONSTANTS["TAU"]

    epsilon = 1e-6
    fa = f(A, delta, v, a)
    fb = f(B, delta, v, a)
    counter = 0
    while abs(B - A) > epsilon:
        C = A + (A - B) * fa / (fb - fa)
        fc = f(C, delta, v, a)
        if fc * fb < 0:
            A = B
            fa = fb
        else:
            fa /= 2
        B = C
        fb = fc
        counter += 1
        if counter > 1000:
            break

    new_volatility = math.exp(A / 2.0)
    phi_star = math.sqrt(phi**2 + new_volatility**2)
    new_phi = 1.0 / math.sqrt(1.0 / phi_star**2 + 1.0 / v)
    if new_phi**2 * delta_sum > 1000.0 / RATING_CONSTANTS["SCALE"]:
        new_mu = mu + 1000.0 / RATING_CONSTANTS["SCALE"]
    elif new_phi**2 * delta_sum < -1000.0 / RATING_CONSTANTS["SCALE"]:
        new_mu = mu - 1000.0 / RATING_CONSTANTS["SCALE"]
    else:
        new_mu = mu + new_phi**2 * delta_sum

    target.new_rating = new_mu * RATING_CONSTANTS["SCALE"] + RATING_CONSTANTS["BASE_RATING"]
    target.new_rd = min(new_phi * RATING_CONSTANTS["SCALE"], RATING_CONSTANTS["MAX_RD"])
    target.volatility = min(new_volatility, RATING_CONSTANTS["MAX_VOLATILITY"])

def extract_player_info(input_filename):
    logger.info(f"Extracting player info from {input_filename}")
    players_dict = {}
    with open(input_filename, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            try:
                player = eval(line)
                fide_id = int(player["id"])
                name = player["name"]
                federation = player["fed"]
                sex = player.get("sex", "M")
                if "w" in player.get("flag", ""):
                    sex = "F"
                b_year = player.get("b_year", "0").split(".")[-1]
                players_dict[fide_id] = {"name": name, "federation": federation, "sex": sex, "b_year": b_year}
            except Exception as e:
                logger.error(f"Error processing player info line: {line} | {e}")
    logger.info(f"Extracted info for {len(players_dict)} players.")
    return players_dict

class RatingListWriter:
    """Handles writing rating lists in various formats"""
    def __init__(self, players, players_info, year):
        self.players = players
        self.players_info = players_info
        self.year = year
        self.sorted_players = sorted(players.values(), key=lambda p: p.rating, reverse=True)

    def write_raw_ratings(self, filename):
        logger.info(f"Writing raw ratings to {filename}")
        lines = [f"{player.id} {player.rating:.7f} {player.rd:.7f} {player.volatility:.7f}\n"
                 for player in self.players.values()]
        with open(filename, "w") as out_file:
            out_file.writelines(lines)

    def _get_player_details(self, player):
        info = self.players_info.get(player.id, {})
        return {
            "name": info.get("name", ""),
            "federation": info.get("federation", ""),
            "sex": info.get("sex", ""),
            "b_year": self._normalize_birth_year(info.get("b_year", "")),
        }

    def _normalize_birth_year(self, b_year):
        if not b_year.isdigit():
            return 0
        year = int(b_year)
        return year + 1900 if year < 100 else year

    def _write_category_file_to_s3(self, bucket, s3_key, header, players_with_rank):
        lines = [header + "\n"]
        for rank, player, details in players_with_rank:
            line = f"{rank} {details['name']}\t{details['federation']} {details['b_year']} "
            if "sex" in details:
                line += f"{details.get('sex', '')} "
            line += f"{player.rating:.7f} {player.rd:.7f} {player.id}\n"
            lines.append(line)
        content = "".join(lines)
        try:
            s3_client.put_object(Bucket=bucket, Key=s3_key, Body=content)
            logger.info(f"Uploaded category file to {s3_key}")
        except Exception as e:
            logger.error(f"Error uploading {s3_key}: {e}")

    def write_global_lists(self, output_prefix, bucket):
        logger.info("Writing global rating lists.")
        categories = {
            "open": {"max_count": 100, "condition": lambda p, d: True},
            "women": {"max_count": 100, "condition": lambda p, d: d["sex"] == "F"},
            "juniors": {"max_count": 100, "condition": lambda p, d: self.year - d["b_year"] <= 20},
            "girls": {"max_count": 100, "condition": lambda p, d: d["sex"] == "F" and self.year - d["b_year"] <= 20},
        }
        for category, settings in categories.items():
            qualified_players = []
            count = 0
            for player in self.sorted_players:
                details = self._get_player_details(player)
                if settings["condition"](player, details):
                    count += 1
                    qualified_players.append((count, player, details))
                    if count >= settings["max_count"]:
                        break
            if qualified_players:
                s3_key = f"{output_prefix}/{category}.txt"
                header = ("Rank Name Federation BirthYear Rating RD" if category in ["women", "girls"]
                          else "Rank Name Federation BirthYear Sex Rating RD")
                self._write_category_file_to_s3(bucket, s3_key, header, qualified_players)
        logger.info("Global rating lists written.")

def apply_new_ratings(players):
    logger.info("Applying new ratings.")
    for player in players.values():
        player.rating = player.new_rating
        player.rd = player.new_rd

def main(ratings_filename, games_filename, output_filename,
         top_rating_list_dir, top_rating_list_filename,
         player_info_filename, year):
    logger.info("Main processing started.")
    try:
        df_games = pd.read_csv(games_filename)
        logger.info(f"Loaded {len(df_games)} game records from CSV.")
    except Exception as e:
        logger.error(f"Failed to read CSV {games_filename}: {e}")
        raise

    players = {}
    logger.info(f"Loading previous ratings from {ratings_filename}")
    try:
        with open(ratings_filename, "r") as rating_file:
            for line in rating_file:
                parts = line.strip().split()
                player_id = int(parts[0])
                rating = float(parts[1])
                rd = float(parts[2])
                volatility = float(parts[3])
                players[player_id] = Player(player_id, rating, rd, volatility)
    except Exception as e:
        logger.info("No previous ratings found. Using default ratings.")

    logger.info(f"Extracting player info from {player_info_filename}")
    players_info = extract_player_info(player_info_filename)

    logger.info("Processing game records.")
    for idx, row in df_games.iterrows():
        try:
            p1 = int(row['Player1'])
            p2 = int(row['Player2'])
            result = float(row['Result'])
        except Exception as ex:
            logger.error(f"Error in row {idx}: {row.to_dict()} | {ex}")
            continue
        add_game(p1, GameResult(opponent_id=p2, score=result), players)
        add_game(p2, GameResult(opponent_id=p1, score=(1.0 - result)), players)
    logger.info("Game records processed.")

    logger.info("Updating player ratings.")
    for player in players.values():
        glicko2_update(player, players)
    apply_new_ratings(players)
    logger.info("Player ratings updated.")

    writer = RatingListWriter(players, players_info, year)
    writer.write_raw_ratings(output_filename)
    logger.info(f"Raw ratings written to {output_filename}")
    writer.write_global_lists(os.path.join(top_rating_list_dir, top_rating_list_filename), bucket=OUTPUT_BUCKET)
    logger.info("Global rating lists written.")
    logger.info("Main processing complete.")

def download_file_from_s3(bucket, key, download_path):
    logger.info(f"Downloading: bucket={bucket}, key={key}")
    s3_client.download_file(bucket, key, download_path)
    logger.info(f"Downloaded {key} to {download_path}")

def upload_file_to_s3(file_path, bucket, key):
    logger.info(f"Uploading {file_path} to bucket {bucket} with key {key}")
    s3_client.upload_file(file_path, bucket, key)
    logger.info("Upload complete.")

def lambda_handler(event, context):
    logger.info(f"Lambda invoked with event: {event}")
    
    # Check for required keys
    if "year" not in event or "month" not in event or "time_control" not in event:
        logger.error("Missing 'year', 'month', or 'time_control' parameters.")
        return {"error": "year, month, and time_control parameters are required."}
    
    try:
        # Retrieve and convert inputs
        year = int(event["year"])
        month = int(event["month"])
        time_control = event["time_control"]
    except Exception as e:
        logger.error(f"Invalid input types: {e}")
        return {"error": "Invalid input types: year and month must be integers."}
    
    # Build S3 keys using the year and month integers
    player_info_key = PLAYER_INFO_KEY_TEMPLATE.format(year=year, month=month)
    games_key = GAMES_KEY_TEMPLATE.format(year=year, month=month, time_control_lower=time_control.lower())
    prev_ratings_key = PREV_RATINGS_KEY_TEMPLATE.format(
        time_control=time_control,
        year=year if month != 1 else year - 1,
        month=month - 1 if month != 1 else 12
    )
    output_key = OUTPUT_RATINGS_KEY_TEMPLATE.format(time_control=time_control, year=year, month=month)
    
    tmp_dir = tempfile.gettempdir()
    ratings_file_path = os.path.join(tmp_dir, "ratings.txt")
    games_file_path = os.path.join(tmp_dir, "games.csv")
    player_info_file_path = os.path.join(tmp_dir, "player_info.txt")
    output_file_path = os.path.join(tmp_dir, "ratings.txt")
    top_rating_list_dir = os.path.join(tmp_dir, "top_rating_lists")
    os.makedirs(top_rating_list_dir, exist_ok=True)
    top_rating_list_filename = f"{year}-{month:02d}"
    
    try:
        download_file_from_s3(PLAYER_INFO_BUCKET, player_info_key, player_info_file_path)
        download_file_from_s3(GAMES_BUCKET, games_key, games_file_path)
    except Exception as e:
        logger.error(f"Error downloading input files: {e}")
        return {"error": "Failed to obtain input files from S3."}
    
    try:
        download_file_from_s3(OUTPUT_BUCKET, prev_ratings_key, ratings_file_path)
    except Exception as e:
        logger.warning(f"Could not download previous ratings: {e}. Continuing without them.")
    
    try:
        main(
            ratings_filename=ratings_file_path,
            games_filename=games_file_path,
            output_filename=output_file_path,
            top_rating_list_dir=top_rating_list_dir,
            top_rating_list_filename=top_rating_list_filename,
            player_info_filename=player_info_file_path,
            year=year
        )
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        return {"error": "Calculation failed."}
    
    try:
        upload_file_to_s3(output_file_path, OUTPUT_BUCKET, output_key)
    except Exception as e:
        logger.error(f"Error uploading output: {e}")
        return {"error": "Failed to upload results to S3."}
    
    logger.info("Lambda processing complete.")
    return {"status": "Success", "year": year, "month": month, "time_control": time_control}
