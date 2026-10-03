import json
import os
import pandas as pd
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from pathlib import Path

load_dotenv()

INPUT_PATH = Path("data/transformed_weather_data.csv")
INDEX_NAME = "weather_data"
MAPPING_FILE = "elasticsearch/mapping.json"

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL")
# ELASTICSEARCH_USERNAME = os.getenv("ELASTICSEARCH_USERNAME")
# ELASTICSEARCH_PASSWORD = os.getenv("ELASTICSEARCH_PASSWORD")

def create_es_client():
  client = Elasticsearch(
    ELASTICSEARCH_URL,
    # basic_auth=(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD)
  )
  print(f"Connected to Elasticsearch at {ELASTICSEARCH_URL}")
  return client

def load_data_to_elasticsearch(df):
  client = create_es_client()
  create_index_if_not_exists(client)
  actions = generate_action(df)
  success, failed = bulk(client, actions, raise_on_error=False)
  print(f"Successfully indexed {success} documents. Failed to index {len(failed)} documents.")
  print(f"Failed documents: {failed}")

def create_index_if_not_exists(client):
  if client.indices.exists(index=INDEX_NAME):
    print(f"Index {INDEX_NAME} already exists.")
    return
  with open(MAPPING_FILE, "r", encoding="utf-8") as f:
    mapping = json.load(f)
  client.indices.create(index=INDEX_NAME, body=mapping)
  print(f"Created index {INDEX_NAME} with mapping from {MAPPING_FILE}")

def read_transformed_data(filename):
  if not Path(filename).exists():
    print(f"Transformed data file {filename} does not exist. Please run the transformation script first.")
    return None
  df = pd.read_csv(filename)
  print(f"Read {len(df)} records from {filename}")
  return df

def generate_action(df):
  for _, row in df.iterrows():
    document = {
      "date": row["date"],
          "mean_temperature": float(row["mean_temperature"]),
          "max_temperature": float(row["max_temperature"]),
          "min_temperature": float(row["min_temperature"]),
          "total_precipitation": float(row["total_precipitation"]),
          "max_wind_speed": float(row["max_wind_speed"])
    }
    yield {
      "_index": INDEX_NAME,
      "_id": row["date"],
      "_source": document
    }
  
  
if __name__ == "__main__":
  transformed_data = read_transformed_data(INPUT_PATH)
  if transformed_data is not None:
    print("Loading data into Elasticsearch...")
    load_data_to_elasticsearch(transformed_data)
    print("Data loading completed.")
