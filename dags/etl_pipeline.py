import subprocess
from datetime import datetime, timedelta
from airflow.decorators import dag,task

PROJECT_DIR = "/opt/airflow"
EXTRACT_SCRIPT = f"{PROJECT_DIR}/scripts/extract.py"
TRANSFORM_SCRIPT = f"{PROJECT_DIR}/scripts/transform.py"
VALIDATE_SCRIPT = f"{PROJECT_DIR}/scripts/validate.py"
LOAD_SCRIPT = f"{PROJECT_DIR}/scripts/load.py"

def start_script(script_path):
  print(f"Starting script: {script_path}")
  result = subprocess.run(["python", script_path], cwd=PROJECT_DIR, capture_output=True, text=True)
  print(f"Script output: {result.stdout}")
  if result.stderr:
    print(f"Script error: {result.stderr}")
  if result.returncode != 0:
    raise RuntimeError(f"Script {script_path} failed with return code {result.returncode}")
  print(f"Script {script_path} completed successfully.")
  
@dag(
  dag_id="weather_pipeline",
  start_date=datetime(2026, 1, 1),
  schedule="@daily",
  catchup=False,
  default_args={
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
  },
  tags=["weather", "etl_pipeline"],
)

def weather_pipeline():
  @task(task_id="extract_script")
  def extract_data():
    start_script(EXTRACT_SCRIPT)

  @task(task_id="transform_script")
  def transform_data():
    start_script(TRANSFORM_SCRIPT)

  @task(task_id="validate_script")
  def validate_data():
    start_script(VALIDATE_SCRIPT)

  @task(task_id="load_script")
  def load_data():
    start_script(LOAD_SCRIPT)
  
  extract = extract_data()
  transform = transform_data()
  validate = validate_data()
  load = load_data()
  
  extract >> transform >> validate >> load

weather_pipeline()
