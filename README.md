# FIDE Glicko

## Pipeline

1. The main pipeline can be found at [manual-run.yml](https://github.com/yetiowin805/chess-glicko-aws/blob/master/.github/workflows/manual-run.yml). This can be run as a GitHub Action.
2. This pipeline will run the [chess-glicko-task](https://us-east-2.console.aws.amazon.com/ecs/v2/task-definitions/chess-glicko-task?region=us-east-2) AWS Elastic Container Service (ECS) task.
3. The task runs the [chess-glicko-pipeline](https://us-east-2.console.aws.amazon.com/ecr/repositories/private/961341531973/chess-glicko-pipeline?region=us-east-2) image in the AWS Elastic Container Registry (ECR).
4. The container runs the script [entrypoint.sh](https://github.com/yetiowin805/chess-glicko-aws/blob/master/entrypoint.sh).
5. The main function is [run_full_pipeline](https://github.com/yetiowin805/chess-glicko-aws/blob/master/entrypoint.sh#L163).
6. The first step is to download the player data for the given month with [download_player_data.py](https://github.com/yetiowin805/chess-glicko-aws/blob/master/src/download_player_data.py). This data is written into an S3 bucket.
7. Then, the scraped data is processed into a SQLite database with [process_fide_rating_list.py](https://github.com/yetiowin805/chess-glicko-aws/blob/master/src/process_fide_rating_list.py). This also writes into an S3 bucket.
8. The next step is to scrape and process all the tournament data for the given month with [tournament_scraper.py](https://github.com/yetiowin805/chess-glicko-aws/blob/master/src/tournament_scraper.py). This also writes into an S3 bucket.
9. Then, it will collect a list of players who have played in a given time control in a given month with [aggregate_player_ids.py](https://github.com/yetiowin805/chess-glicko-aws/blob/master/src/aggregate_player_ids.py). It writes the result as a sorted text file into an S3 bucket.
10. With a Rust script, [scrape_calculations.rs](https://github.com/yetiowin805/chess-glicko-aws/blob/master/rust-src/src/scrape_calculations.rs) will scrape the games played by each active player and add them to a JSONL file that is written into an S3 bucket. Note that this should be updated to keep track of color information.
11. We then use [process_calculations.rs](https://github.com/yetiowin805/chess-glicko-aws/blob/master/rust-src/src/process_calculations.rs) to process the JSONL to write all the games into a file and save it in S3. This should be changed to include color information.
12. Rating changes are computed with [glicko.rs](https://github.com/yetiowin805/chess-glicko-aws/blob/master/rust-src/src/glicko.rs). This should be updated to write the results into a database and include a "marginal contribution" column. Each game should be written twice initially, but at the end we can dedupe (and have 2 rating changes per game).