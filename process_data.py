import os
import gzip
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

LOG_FILE = "logs/process.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CSV_URL = "https://tyroo-engineering-assesments.s3.us-west-2.amazonaws.com/Tyroo-dummy-data.csv.gz"
LOCAL_FILE = "Tyroo-dummy-data.csv.gz"
CHUNK_SIZE = 100000
TABLE_NAME = "tyroo_data"
DB_URI = "postgresql+psycopg2://tyroo_user:tyroo_pass@localhost:5432/tyroo_db"

def download_csv(url, local_path):
  try:
    logging.info("Downloading CSV file...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(local_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    logging.info("Download complete.")
  except requests.RequestException as e:
    logging.error(f"Download failed: {e}")
    raise

def process_chunk(chunk):
  try:
    chunk.dropna(inplace=True)
    chunk.columns = [col.strip().lower().replace(" ", "_") for col in chunk.columns]
    if 'date' in chunk.columns:
        chunk['date'] = pd.to_datetime(chunk['date'], errors='coerce')
    return chunk
  except Exception as e:
    logging.error(f"Chunk processing failed: {e}")
    raise

def load_to_db(df, engine):
  try:
    df.to_sql(TABLE_NAME, con=engine, if_exists='append', index=False)
    logging.info(f"Inserted {len(df)} rows into {TABLE_NAME}.")
  except SQLAlchemyError as e:
    logging.error(f"Database insert failed: {e}")
    raise

def main():
  download_csv(CSV_URL, LOCAL_FILE)
  engine = create_engine(DB_URI)

  try:
    with gzip.open(LOCAL_FILE, 'rt') as f:
        reader = pd.read_csv(f, chunksize=CHUNK_SIZE)
        for i, chunk in enumerate(reader):
            logging.info(f"Processing chunk {i}")
            cleaned = process_chunk(chunk)
            load_to_db(cleaned, engine)
  except Exception as e:
    logging.error(f"Main processing failed: {e}")
    raise

if __name__ == "__main__":
  main()
