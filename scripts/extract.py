import os
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import requests
import pandas as pd

load_dotenv()

api_url = os.getenv("API_URL")

LATITUDE = -18.8792
LONGITUDE = 47.5079

START_DATE = "2026-09-01"
END_DATE = datetime.now(ZoneInfo("Indian/Antananarivo")).strftime("%Y-%m-%d")

def fetch_weather_data(latitude, longitude, start_date, end_date):
  params = {
    "latitude": latitude,
    "longitude": longitude,
    "start_date": start_date,
    "end_date": end_date,
    "daily": ["temperature_2m_mean","temperature_2m_max", "temperature_2m_min", "precipitation_sum", "windspeed_10m_max", "rain_sum"],
    "timezone": "Indian/Antananarivo",
  }

  response = requests.get(api_url, params=params)
  response.raise_for_status()
  data = response.json()
  df = pd.DataFrame(data['daily'])
  return df

def save_to_csv(df, filename):
  df.to_csv(filename, index=False)
  print(f"Data saved to {filename}")
  
if __name__ == "__main__":
  print(f"Fetching weather data for coordinates: ({LATITUDE}, {LONGITUDE}) from {START_DATE} to {END_DATE}")
  weather_data = fetch_weather_data(LATITUDE, LONGITUDE, START_DATE, END_DATE)
  
  print("Weather data fetched successfully. Saving to CSV...")
  save_to_csv(weather_data, "data/weather_data.csv")
