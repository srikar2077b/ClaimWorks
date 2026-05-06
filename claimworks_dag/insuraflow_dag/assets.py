from dagster import asset, Output
import papermill as pm
import os
import yaml
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import numpy as np


# Load dynamic configuration
with open("D:\Data Engineering Prep\Pyspark\config\config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Set up logger
log_file = config["log_file"]
os.makedirs(os.path.dirname(log_file), exist_ok=True)

logger = logging.getLogger("dagster_assets")
if not logger.hasHandlers():
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter("[DAGSTER] %(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

@asset
def run_cleaning_notebook() -> Output[str]:
    config_path = os.getenv("CLAIMWORKS_CONFIG_PATH")
    if not config_path or not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")
    
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
        
    input_nb = config["notebooks"]["cleaning"]["input"]
    temp_nb = config["notebooks"]["cleaning"]["output_temp"]
    input_path = config["paths"]["raw_data"]
    output_json = config["paths"]["cleaned_data"]

    try:
        logger.info(f"Executing cleaning notebook: {input_nb}")
        pm.execute_notebook(
            input_path=input_nb,
            output_path=temp_nb,
            parameters={
                "input_path": input_path,
                "output_path": output_json,
                "config_path": config_path
            }
        )
        if not os.path.exists(output_json):
            raise FileNotFoundError(f"Expected output file not found: {output_json}")
        logger.info(f"Cleaning notebook completed. Output written to: {output_json}")
        return Output(output_json)
    finally:
        if os.path.exists(temp_nb):
            os.remove(temp_nb)
            logger.info(f"Temporary notebook removed: {temp_nb}")


@asset
def run_transformation_notebook(run_cleaning_notebook: str) -> Output[str]:
    config_path = os.getenv("CLAIMWORKS_CONFIG_PATH")
    if not config_path or not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")
    
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
        
        
    input_nb = config["notebooks"]["transforming"]["input"]
    temp_nb = config["notebooks"]["transforming"]["output_temp"]
    input_path = config["paths"]["cleaned_data"]
    output_json = config["paths"]["transformed_data"]

    try:
        logger.info(f"Executing transformation notebook: {input_nb}")
        pm.execute_notebook(
            input_path=input_nb,
            output_path=temp_nb,
            parameters={
                "input_path": input_path,
                "output_path": output_json
            }
        )
        if not os.path.exists(output_json):
            raise FileNotFoundError(f"Expected output file not found: {output_json}")
        logger.info(f"Transformation notebook completed. Output written to: {output_json}")
        return Output(output_json)
    finally:
        if os.path.exists(temp_nb):
            os.remove(temp_nb)
            logger.info(f"Temporary notebook removed: {temp_nb}")



@asset
def upload_to_postgres(run_transformation_notebook: str) -> None:
    config_path = os.getenv("CLAIMWORKS_CONFIG_PATH")
    if not config_path or not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found at {config_path}")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    db_config = config.get("database", {})
    db_url = db_config.get("url")
    table_name = db_config.get("table_name", "insurance_data")

    if not db_url:
        raise ValueError("Database connection URL not found in config")

    transformed_file = config["paths"]["transformed_data"]
    if not os.path.exists(transformed_file):
        raise FileNotFoundError(f"Transformed JSON not found at {transformed_file}")

    conn = None
    cursor = None

    try:
        logger.info(f"[UPLOAD] Connecting to PostgreSQL database via Supabase")
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()

        logger.info(f"[UPLOAD] Detecting UUID columns in table: {table_name}")
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s AND data_type = 'uuid';
        """, (table_name,))
        uuid_columns = {row[0] for row in cursor.fetchall()}
        logger.info(f"[UPLOAD] Detected UUID columns: {uuid_columns}")

        logger.info(f"[UPLOAD] Truncating existing table: {table_name}")
        cursor.execute(f"TRUNCATE TABLE {table_name};")
        conn.commit()

        logger.info(f"[UPLOAD] Reading transformed data from: {transformed_file}")
        df = pd.read_json(transformed_file, lines=True)

        # Clean invalid values before insertion
        logger.info(f"[UPLOAD] Cleaning invalid values from DataFrame")
        df.replace(["None", "", np.nan], None, inplace=True)

        # Replace empty string or "None" with None for UUID columns
        records = []
        for row in df.to_dict(orient='records'):
            for col in uuid_columns:
                if col in row and (row[col] == "" or row[col] == "None"):
                    row[col] = None
            records.append(tuple(row.values()))

        columns = df.columns.tolist()
        insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"

        logger.info(f"[UPLOAD] Preparing {len(records)} rows for batch insertion")
        try:
            psycopg2.extras.execute_values(cursor, insert_sql, records, page_size=1000)
        except Exception as e:
            logger.error("[UPLOAD] Bulk insert failed.")
            logger.exception(e)
            raise

        conn.commit()
        logger.info("[UPLOAD] Data successfully ingested using batch insert.")

    except Exception as e:
        logger.error(f"[UPLOAD] Failed during PostgreSQL ingestion: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logger.info("[UPLOAD] PostgreSQL connection closed.")
