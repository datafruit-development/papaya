#!/bin/bash

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$DEMO_DIR/.venv_spark_demo_scratch"
PYTHON_EXEC="$VENV_DIR/bin/python3"
METRICS_OUTPUT_BASE_DIR="/tmp/spark_skew_demo_metrics" # Centralized metrics output
LOG_OUTPUT_BASE_DIR="/tmp/papaya_job_logs" # Centralized Papaya custom logs

export PYSPARK_PYTHON="$PYTHON_EXEC"
export PYSPARK_DRIVER_PYTHON="$PYTHON_EXEC" # Also ensure driver uses the venv python

# Function to print styled messages
print_info() { echo -e "\033[1;34m[INFO]\033[0m $1"; }
print_warn() { echo -e "\033[1;33m[WARN]\033[0m $1"; }
print_error() { echo -e "\033[1;31m[ERROR]\033[0m $1"; }
print_success() { echo -e "\033[1;32m[SUCCESS]\033[0m $1"; }
print_header() {
    echo -e "\n\033[1;35m======================================================================\033[0m"
    echo -e "\033[1;34m$1\033[0m"
    echo -e "\033[1;35m======================================================================\033[0m\n"
}

trap 'print_error "An error occurred. Exiting."; exit 1' ERR

# Check for uv
if ! command -v uv &> /dev/null; then
    print_error "uv is not installed or not in PATH. Please install uv: https://github.com/astral-sh/uv"
    exit 1
fi

# Welcome Message
clear
print_header "Spark Data Skew Demo with Papaya Collectors"
cat << EOF
This demo will:
1. Set up PostgreSQL & load skewed data.
2. Run a SKEWED Spark job, collecting metrics with Papaya.
3. Run a FIXED Spark job (Broadcast), collecting metrics.
4. Run a FIXED Spark job (Salting), collecting metrics.
5. Metrics will be saved in '$METRICS_OUTPUT_BASE_DIR'.
   Papaya's CustomLoggingCollector logs (if any specific files created by it) will be under '$LOG_OUTPUT_BASE_DIR'.
EOF

# 1. Setup Environment
print_header "Step 1: Setting up Environment"
mkdir -p "$DEMO_DIR/data"
mkdir -p "$METRICS_OUTPUT_BASE_DIR"
mkdir -p "$LOG_OUTPUT_BASE_DIR"

REGIONS_CSV="$DEMO_DIR/data/regions.csv"
if [ ! -f "$REGIONS_CSV" ]; then
    print_warn "$REGIONS_CSV not found. Creating a dummy one."
    "$PYTHON_EXEC" -c "import pandas as pd; pd.DataFrame({'region_id': list(range(1, 11)), 'region_name': [f'RegionName{i}' for i in range(1, 11)], 'country': ['USA']*6 + ['Canada']*2 + ['Mexico']*2}).to_csv('$REGIONS_CSV', index=False)"
    print_success "Dummy regions.csv created."
fi

if [ ! -d "$VENV_DIR" ]; then
    print_info "Creating uv virtual environment at $VENV_DIR..."
    uv venv "$VENV_DIR" --seed # Add --seed for system site packages if needed for pyspark globally
    print_success "Virtual environment created."
else
    print_info "Virtual environment '$VENV_DIR' already exists."
fi

print_info "Installing/Verifying Python requirements using uv..."
uv pip install -p "$PYTHON_EXEC" -r "$DEMO_DIR/setup/requirements.txt"
print_success "Requirements checked/installed."

# 2. Start PostgreSQL
print_header "Step 2: Starting PostgreSQL Container"
docker-compose -f "$DEMO_DIR/setup/docker-compose.yml" up -d
print_info "Waiting for PostgreSQL to be ready (can take up to a minute)..."
MAX_RETRIES=12
RETRY_COUNT=0
until docker exec skew_demo_postgres pg_isready -U demo_user -d skew_db -q 2>/dev/null; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ "$RETRY_COUNT" -ge "$MAX_RETRIES" ]; then
        print_error "PostgreSQL did not become ready in time. Check Docker logs."
        docker-compose -f "$DEMO_DIR/setup/docker-compose.yml" logs postgres
        exit 1
    fi
    echo -n "." && sleep 5
done
echo && print_success "PostgreSQL container is ready."

# 3. Populate Database
print_header "Step 3: Populating Database with Skewed Data"
"$PYTHON_EXEC" "$DEMO_DIR/setup/setup_database.py"
print_success "Database populated."

# Variables to store metrics file paths
SKEWED_JOB_METRICS_FILE=""
FIXED_BROADCAST_METRICS_FILE=""
FIXED_SALTING_METRICS_FILE=""

# 4. Run Skewed Spark Job with Collectors
print_header "Step 4: Running SKEWED Spark Job with Papaya Collectors"
"$PYTHON_EXEC" "$DEMO_DIR/execute_spark_job.py" skewed --job-name "SkewedJoinDemo"
SKEWED_JOB_METRICS_FILE=$(ls -t $METRICS_OUTPUT_BASE_DIR/metrics_SkewedJoinDemo_*.json | head -1)
if [ -f "$SKEWED_JOB_METRICS_FILE" ]; then
    print_success "Skewed Spark job finished. Metrics at: $SKEWED_JOB_METRICS_FILE"
else
    print_warn "Skewed Spark job finished, but metrics file not found as expected."
fi

# 5. Run Fixed Spark Job (Broadcast) with Collectors
print_header "Step 5: Running FIXED Spark Job (Broadcast) with Papaya Collectors"
"$PYTHON_EXEC" "$DEMO_DIR/execute_spark_job.py" fixed_broadcast --job-name "FixedJoinBroadcastDemo"
FIXED_BROADCAST_METRICS_FILE=$(ls -t $METRICS_OUTPUT_BASE_DIR/metrics_FixedJoinBroadcastDemo_*.json | head -1)
if [ -f "$FIXED_BROADCAST_METRICS_FILE" ]; then
    print_success "Fixed Spark job (Broadcast) finished. Metrics at: $FIXED_BROADCAST_METRICS_FILE"
else
    print_warn "Fixed Spark job (Broadcast) finished, but metrics file not found as expected."
fi

# 6. Run Fixed Spark Job (Salting) with Collectors
print_header "Step 6: Running FIXED Spark Job (Salting) with Papaya Collectors"
"$PYTHON_EXEC" "$DEMO_DIR/execute_spark_job.py" fixed_salting --job-name "FixedJoinSaltingDemo"
FIXED_SALTING_METRICS_FILE=$(ls -t $METRICS_OUTPUT_BASE_DIR/metrics_FixedJoinSaltingDemo_*.json | head -1)
if [ -f "$FIXED_SALTING_METRICS_FILE" ]; then
    print_success "Fixed Spark job (Salting) finished. Metrics at: $FIXED_SALTING_METRICS_FILE"
else
    print_warn "Fixed Spark job (Salting) finished, but metrics file not found as expected."
fi

print_header "Demo Summary"
echo "Collected metrics files:"
echo "  Skewed Job:        ${SKEWED_JOB_METRICS_FILE:-Not found}"
echo "  Fixed (Broadcast): ${FIXED_BROADCAST_METRICS_FILE:-Not found}"
echo "  Fixed (Salting):   ${FIXED_SALTING_METRICS_FILE:-Not found}"
echo ""
echo "These JSON files contain the data collected by Papaya and can be used for analysis and visualization."
echo "Next steps would involve using these files to generate reports/visualizations as outlined in your demo flow."
echo ""
print_info "Demo conceptual flow completed."
print_warn "To clean up Docker resources, run: docker-compose -f \"$DEMO_DIR/setup/docker-compose.yml\" down -v"