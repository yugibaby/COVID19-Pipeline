import os
from dotenv import load_dotenv
from dagster import op, job, DynamicOut, DynamicOutput, RetryPolicy
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from io import StringIO
import pandera as pa
from pandera import DataFrameSchema, Column

# Load environment variables from .env file
load_dotenv()

# Fetch sensitive data from environment variables
POSTGRES_URL = os.getenv("DATABASE_URL")  
GITHUB_TOKEN = os.getenv("GITHUB_API_TOKEN")  

@op(description="Fetches the list of available CSV filenames from the GitHub COVID-19 data repository.")
def github_api_list(context):
    base_url = "https://api.github.com/repos/CSSEGISandData/COVID-19/contents/csse_covid_19_data/csse_covid_19_daily_reports"
    headers = {"Authorization": f"token {GITHUB_TOKEN}"}
    response = requests.get(base_url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch file list: {response.status_code}")
    file_data = response.json()
    file_urls = [file_info["download_url"] for file_info in file_data if file_info["name"].endswith(".csv")]
    return file_urls

@op(out=DynamicOut(), description="Emits each file URL as a separate dynamic output for parallel processing.")
def emit_urls(context, file_urls):
    for idx, file_url in enumerate(file_urls):
        yield DynamicOutput(file_url, mapping_key=str(idx))

@op(retry_policy=RetryPolicy(max_retries=3, delay=5), description="Fetches a single CSV file from GitHub with retry logic.")
def fetch_csv_with_retry(context, file_url):
    response = requests.get(file_url)
    if response.status_code != 200:
        context.log.warning(f"Failed to fetch CSV: {response.status_code}")
        return None
    return response.text

@op(description="Validates and processes raw CSV data to ensure it conforms to the expected schema.")
def validate_and_process_data(context, raw_csv):
    if raw_csv is None or not raw_csv.strip():
        context.log.warning("Raw CSV is None or empty. Skipping validation and processing.")
        return None

    # Define the schema for validation
    schema = DataFrameSchema({
        "fips": Column(pa.Float, nullable=True),
        "admin2": Column(pa.String, nullable=True),
        "province_state": Column(pa.String, nullable=True),
        "country_region": Column(pa.String, nullable=True),
        "last_update": Column(pa.Timestamp, nullable=True),
        "lat": Column(pa.Float, nullable=True),
        "long_": Column(pa.Float, nullable=True),
        "confirmed": Column(pa.Float, nullable=True),
        "deaths": Column(pa.Float, nullable=True),
        "recovered": Column(pa.Float, nullable=True),
        "active": Column(pa.Float, nullable=True),
        "combined_key": Column(pa.String, nullable=True),
        "incident_rate": Column(pa.Float, nullable=True),
        "case_fatality_ratio": Column(pa.Float, nullable=True),
    })

    try:
        df = pd.read_csv(StringIO(raw_csv))
        
        # Normalize column names
        df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace("-", "_").str.replace("/", "_")
        df.rename(columns={"latitude": "lat", "longitude": "long_", "incidence_rate": "incident_rate"}, inplace=True)

        # Add missing columns
        for col in schema.columns:
            if col not in df.columns:
                df[col] = None

        # Enforce datetime to last_update column
        if "last_update" in df.columns:
            df["last_update"] = pd.to_datetime(df["last_update"], errors="coerce")

        # Enforce float to numerical columns
        numeric_cols = ["fips", "lat", "long_", "confirmed", "deaths", "recovered", "active", "incident_rate", "case_fatality_ratio"]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].astype("float64", errors="ignore")

        # Skipe recovered only data
        if set(df.columns) == {"recovered"}:
            context.log.info("CSV contains only the 'recovered' column. Skipping this file.")
            return None  

        validated_df = schema.validate(df)
        return validated_df

    except Exception as e:
        context.log.warning(f"Validation and processing failed: {str(e)}")
        return None

@op(description="Loads the processed DataFrame into the `raw.covid_data` table in PostgreSQL.")
def load_data_to_postgres(context, df):
    if df is None or df.empty:
        context.log.warning("No data to load into PostgreSQL.")
        return

    try:
        engine = create_engine(POSTGRES_URL)
        context.log.info("Loading data into PostgreSQL...")
        with engine.connect() as connection:
            # Ensure the schema exists
            connection.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
            
            # Load data into the table
            df.to_sql(
                name="covid_data",
                con=connection,
                schema="raw",
                if_exists="append",
                index=False
            )
        context.log.info("Data loaded successfully into 'raw.covid_data'.")
    except Exception as e:
        context.log.error(f"Failed to load data to PostgreSQL: {str(e)}")
        raise

@job(description="ETL pipeline that fetches, validates, processes, and loads COVID-19 data into PostgreSQL.")
def covid_data_pipeline():
    file_urls = github_api_list()
    file_url_outputs = emit_urls(file_urls)

    (
        file_url_outputs
        .map(fetch_csv_with_retry)
        .map(validate_and_process_data)
        .map(load_data_to_postgres)
    )
