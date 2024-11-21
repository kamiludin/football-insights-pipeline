# Import libraries and modules
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from io import StringIO
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago

# Define constants and variables
AWS_CONN_ID = "YOUR_AWS_CONN_ID"
REDSHIFT_CONN_ID = "YOUR_REDSHIFT_CONN_ID"
S3_BUCKET_NAME = "YOUR_S3_BUCKET_NAME"
S3_KEY_PREFIX = "understat_data"

# Define league list
LEAGUES = {
    "EPL": "epl",
    "La_Liga": "laliga",
    "Bundesliga": "bundesliga",
    "Serie_A": "serie_a",
    "Ligue_1": "ligue1"
}

# Function to extract data and load it to S3
def extract_and_load_to_s3(**kwargs):
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    # Helper function to parse JSON data from script tags
    def get_json_from_script(scripts, index):
        script_content = scripts[index].string
        json_data = script_content[script_content.index("('") + 2 : script_content.index("')")]
        return json.loads(json_data.encode('utf8').decode('unicode_escape'))

    # Loop for each league
    for league, label in LEAGUES.items():
        url = f'https://understat.com/league/{league}/2023'
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'lxml')
        scripts = soup.find_all('script')

        # Scrape league table data
        league_table_data = get_json_from_script(scripts, 1)
        df_league_table = pd.DataFrame(league_table_data)

        # Scrape player stats data
        players_stats_data = get_json_from_script(scripts, 3)
        df_players_stats = pd.DataFrame(players_stats_data)

        # Convert DataFrames to CSV format in memory
        league_table_csv = StringIO()
        df_league_table.to_csv(league_table_csv, index=False, encoding='utf-8')
        league_table_csv.seek(0)

        players_stats_csv = StringIO()
        df_players_stats.to_csv(players_stats_csv, index=False, encoding='utf-8')
        players_stats_csv.seek(0)

        # Upload CSVs to S3
        s3_key_league = f"{S3_KEY_PREFIX}/{label}_league_table.csv"
        s3_key_player = f"{S3_KEY_PREFIX}/{label}_players_stats.csv"
        s3_hook.load_string(league_table_csv.getvalue(), s3_key_league, bucket_name=S3_BUCKET_NAME, replace=True)
        s3_hook.load_string(players_stats_csv.getvalue(), s3_key_player, bucket_name=S3_BUCKET_NAME, replace=True)

# Define the DAG
with DAG(
    dag_id="understat_to_redshift_pipeline",
    default_args={"owner": "airflow", "start_date": days_ago(1)},
    concurrency=2,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Task to extract data and load it to S3
    el_to_s3_task = PythonOperator(
        task_id="extract_and_load_to_s3",
        python_callable=extract_and_load_to_s3,
    )

    # Loop through each league to create tables and copy data
    for league, label in LEAGUES.items():
        # Create league table task
        create_league_table_task = SQLExecuteQueryOperator(
            task_id=f"create_{label}_league_table",
            conn_id=REDSHIFT_CONN_ID,
            sql=f"""
            CREATE TABLE IF NOT EXISTS public.{label}_league_table (
                id INTEGER,
                isresult BOOLEAN,
                h VARCHAR(65535),
                a VARCHAR(65535),
                goals VARCHAR(255),
                xg VARCHAR(255),
                datetime VARCHAR(255),
                forecast VARCHAR(255)
            );
            """,
            hook_params={"schema": "public"}
        )

        # Create player stats table task
        create_player_table_task = SQLExecuteQueryOperator(
            task_id=f"create_{label}_player_table",
            conn_id=REDSHIFT_CONN_ID,
            sql=f"""
            CREATE TABLE IF NOT EXISTS public.{label}_player_table (
                id VARCHAR(50),
                player_name VARCHAR(100),
                games INTEGER,
                time INTEGER,
                goals INTEGER,
                xg DOUBLE PRECISION,
                assists INTEGER,
                xa DOUBLE PRECISION,
                shots INTEGER,
                key_passes INTEGER,
                yellow_cards INTEGER,
                red_cards INTEGER,
                position VARCHAR(50),
                team_title VARCHAR(100),
                npg INTEGER,
                npxg DOUBLE PRECISION,
                xgchain DOUBLE PRECISION,
                xgbuildup DOUBLE PRECISION
            );
            """,
            hook_params={"schema": "public"}
        )

        # Copy league table data from S3 to Redshift
        load_league_table_task = S3ToRedshiftOperator(
            task_id=f"load_{label}_league_table_to_redshift",
            schema="public",
            table=f"{label}_league_table",
            s3_bucket=S3_BUCKET_NAME,
            s3_key=f"{S3_KEY_PREFIX}/{label}_league_table.csv",
            redshift_conn_id=REDSHIFT_CONN_ID,
            aws_conn_id=AWS_CONN_ID,
            copy_options=['CSV', 'IGNOREHEADER 1'],
        )

        # Copy player stats data from S3 to Redshift
        load_player_table_task = S3ToRedshiftOperator(
            task_id=f"load_{label}_player_table_to_redshift",
            schema="public",
            table=f"{label}_player_table",
            s3_bucket=S3_BUCKET_NAME,
            s3_key=f"{S3_KEY_PREFIX}/{label}_players_stats.csv",
            redshift_conn_id=REDSHIFT_CONN_ID,
            aws_conn_id=AWS_CONN_ID,
            copy_options=['CSV', 'IGNOREHEADER 1'],
        )

        # Set task dependencies
        el_to_s3_task >> create_league_table_task >> load_league_table_task
        el_to_s3_task >> create_player_table_task >> load_player_table_task