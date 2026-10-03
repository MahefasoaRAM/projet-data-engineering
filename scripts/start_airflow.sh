#!/bin/bash

set -e

echo "========================================="
echo "Initialisation d'Airflow"
echo "========================================="

echo "Migration de la base de données..."
airflow db migrate

echo "Configuration de l'utilisateur Airflow..."

python - <<'PY'
import json
import os

username = os.environ["AIRFLOW_ADMIN_USERNAME"]
password = os.environ["AIRFLOW_ADMIN_PASSWORD"]

password_file = "/opt/airflow/simple_auth_manager_passwords.json.generated"

with open(password_file, "w") as f:
    json.dump({username: password}, f, indent=2)

print(f"Utilisateur configuré : {username}")
PY

echo "Démarrage d'Airflow..."
exec airflow standalone