#!/bin/bash
set -e

echo "Starting Airflow entrypoint..."

# Upgrade pip and requirements
python -m pip install --upgrade pip
if [ -f "/opt/airflow/requirements.txt" ]; then
    echo "Installing Python dependencies..."
    pip install --no-cache-dir -r /opt/airflow/requirements.txt
fi

# Wait for Postgres network port to open
echo "Waiting for Postgres network port..."
while ! nc -z postgres 5432; do
  sleep 1
done

# --- 🟢 WEBSERVER LOGIC ---
if [ "$1" = "webserver" ]; then
    # Delete any old lock files from previous runs
    rm -f /opt/airflow/dags/.db_initialized.lock
    
    echo "Initializing Airflow Database (Webserver Task)..."
    airflow db init

    echo "Creating admin user..."
    airflow users create \
        --username admin \
        --firstname admin \
        --lastname admin \
        --role Admin \
        --email admin@example.com \
        --password admin || true
        
    # 🟢 SIGNAL: Create a lock file in the shared dags folder to tell the scheduler we are done!
    touch /opt/airflow/dags/.db_initialized.lock
fi

# --- 🟢 SCHEDULER LOGIC ---
if [ "$1" = "scheduler" ]; then
    echo "Waiting for Webserver to finish initializing the database tables..."
    # Loop continuously until the webserver creates the lock file
    while [ ! -f /opt/airflow/dags/.db_initialized.lock ]; do
        sleep 2
    done
    echo "Webserver database initialization confirmed! Starting scheduler..."
fi

# Start requested Airflow service
exec airflow "$@"