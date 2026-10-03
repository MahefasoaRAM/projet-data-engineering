import sys
import pandas as pd
from pathlib import Path

INPUT_FILE = Path("data/transformed_weather_data.csv")

REQUIRED_COLUMNS = [
  'date',
  'mean_temperature',
  'max_temperature',
  'min_temperature',
  'total_precipitation',
  'max_wind_speed',
]

NUMERIC_COLUMNS = [
  'mean_temperature',
  'max_temperature',
  'min_temperature',
  'total_precipitation',
  'max_wind_speed',
]

def validate_weather_data(df):
  missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
  if missing_columns:
    raise ValueError(f"Missing required columns: {missing_columns}")

  for col in NUMERIC_COLUMNS:
    if not pd.api.types.is_numeric_dtype(df[col]):
      raise TypeError(f"Column '{col}' must be numeric.")
    
  duplicate_rows = df.duplicated().sum()
  if duplicate_rows > 0:
    raise ValueError(f"Data contains {duplicate_rows} duplicate rows.")
  
  df["date"] = pd.to_datetime(df["date"], errors='coerce')
  if df["date"].isnull().any():
    raise ValueError("Column 'date' contains invalid date formats.")

  if df.isnull().values.any():
    raise ValueError("Data contains missing values.")

  print("Data validation passed successfully.")
  
if __name__ == "__main__":
  if not INPUT_FILE.exists():
    print(f"Input file {INPUT_FILE} does not exist. Please run the transformation script first.")
    sys.exit(1)
  else:
    print(f"Reading data from {INPUT_FILE}")
    weather_data = pd.read_csv(INPUT_FILE)
    
    print("Validating weather data...")
    try:
      validate_weather_data(weather_data)
    except (ValueError, TypeError) as e:
      print(f"Data validation failed: {e}")
      sys.exit(1)
    else:
      print("Data validation completed successfully.")
