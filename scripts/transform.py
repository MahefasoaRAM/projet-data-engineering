import pandas as pd
from pathlib import Path

INPUT_FILE = Path("data/weather_data.csv")
OUTPUT_FILE = Path("data/transformed_weather_data.csv")

def transform_weather_data(df):
  df = df.drop_duplicates()
  df['time'] = pd.to_datetime(df['time']).dt.date
  numeric_columns = ['temperature_2m_mean', 'temperature_2m_max', 'temperature_2m_min', 'precipitation_sum', 'windspeed_10m_max']
  
  for col in numeric_columns:
    missing_values = df[col].isna().sum()
    if missing_values > 0:
      median_value = df[col].median()
      df[col] = df[col].fillna(median_value)
      
  if "rain_sum" in df.columns:
    df = df.drop(columns=['rain_sum'])
  
  df = df.rename(columns={
    'time': 'date',
    'temperature_2m_mean': 'mean_temperature',
    'temperature_2m_max': 'max_temperature',
    'temperature_2m_min': 'min_temperature',
    'precipitation_sum': 'total_precipitation',
    'windspeed_10m_max': 'max_wind_speed',
  })
  return df

def save_transformed_data(df, filename):
  df.to_csv(filename, index=False)
  print(f"Transformed data saved to {filename}")
  
if __name__ == "__main__":
  if not INPUT_FILE.exists():
    print(f"Input file {INPUT_FILE} does not exist. Please run the extraction script first.")
  else:
    print(f"Reading data from {INPUT_FILE}")
    weather_data = pd.read_csv(INPUT_FILE)
    
    print("Transforming weather data...")
    transformed_data = transform_weather_data(weather_data)
    
    print("Saving transformed data to CSV...")
    save_transformed_data(transformed_data, OUTPUT_FILE)