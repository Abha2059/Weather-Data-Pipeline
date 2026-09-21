#!/usr/bin/env bash
# ==============================================================================
# Weather Data Aggregation Pipeline — AWS EC2 Deployment & Bootstrap Script
#
# This script provisions an AWS EC2 instance (Amazon Linux 2023 or Ubuntu)
# with Docker Engine, Docker Compose, system swap memory, and launches
# the multi-container Airflow and Streamlit data pipeline stack.
# ==============================================================================

set -euo pipefail

echo "=================================================="
echo "Starting Weather Data Pipeline AWS EC2 Deployment"
echo "=================================================="

# ------------------------------------------------------------------------------
# 1. OS Detection and System Update
# ------------------------------------------------------------------------------
echo "[Step 1/6] Detecting operating system..."

OS_NAME="$(grep -E '^ID=' /etc/os-release | cut -d= -f2 | tr -d '\"')"
echo "Detected OS: ${OS_NAME}"

if [[ "${OS_NAME}" == "amzn" || "${OS_NAME}" == "al2023" ]]; then
    echo "Updating system packages via dnf..."
    sudo dnf update -y
    sudo dnf install -y git curl procps
elif [[ "${OS_NAME}" == "ubuntu" || "${OS_NAME}" == "debian" ]]; then
    echo "Updating system packages via apt..."
    sudo apt-get update -y
    sudo apt-get install -y git curl procps ca-certificates gnupg lsb-release
else
    echo "Warning: Unrecognized OS '${OS_NAME}'. Attempting standard package install..."
fi

# ------------------------------------------------------------------------------
# 2. Swap Memory Allocation (Vital for t3.small / t3.medium stability)
# ------------------------------------------------------------------------------
echo "[Step 2/6] Configuring virtual swap memory (2GB)..."

if [ ! -f /swapfile ]; then
    echo "Allocating 2GB swap file at /swapfile..."
    sudo dd if=/dev/zero of=/swapfile bs=128M count=16 status=progress
    sudo chmod 600 /swapfile
    sudo mkswap /swapfile
    sudo swapon /swapfile
    echo '/swapfile swap swap defaults 0 0' | sudo tee -a /etc/fstab
    echo "Swap memory allocated and enabled successfully."
else
    echo "Swap file already exists. Skipping allocation."
fi
free -h

# ------------------------------------------------------------------------------
# 3. Docker Engine & Docker Compose Installation
# ------------------------------------------------------------------------------
echo "[Step 3/6] Installing Docker and Docker Compose plugin..."

if ! command -v docker &> /dev/null; then
    if [[ "${OS_NAME}" == "amzn" || "${OS_NAME}" == "al2023" ]]; then
        sudo dnf install -y docker
        sudo systemctl enable --now docker
    elif [[ "${OS_NAME}" == "ubuntu" || "${OS_NAME}" == "debian" ]]; then
        sudo install -m 0755 -d /etc/apt/keyrings
        curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
        sudo chmod a+r /etc/apt/keyrings/docker.gpg
        echo \
          "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
          $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
          sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
        sudo apt-get update -y
        sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
        sudo systemctl enable --now docker
    fi
    sudo usermod -aG docker "$USER" || true
    echo "Docker installed successfully."
else
    echo "Docker is already installed: $(docker --version)"
fi

# Ensure docker service is active
sudo systemctl start docker

# ------------------------------------------------------------------------------
# 4. Project Directory & Environment Verification
# ------------------------------------------------------------------------------
echo "[Step 4/6] Verifying project workspace and configuration..."

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_DIR}"
echo "Working directory: ${PROJECT_DIR}"

if [ ! -f .env ]; then
    echo "ERROR: .env file not found in ${PROJECT_DIR}."
    echo "Please create a .env file with the following variables before proceeding:"
    echo "  WEATHER_API_KEY=<your_key>"
    echo "  AWS_REGION=ap-south-1"
    echo "  AWS_S3_BUCKET=weather-data-pipeline-abhay-699258776334"
    echo "  MYSQL_HOST=<your_rds_endpoint>"
    echo "  MYSQL_PORT=3306"
    echo "  MYSQL_USER=<your_rds_username>"
    echo "  MYSQL_PASSWORD=<your_rds_password>"
    echo "  MYSQL_DATABASE=weather_db"
    exit 1
fi
echo ".env file detected."

# Create staging directories if missing
mkdir -p data/raw data/processed data/powerbi logs airflow

# ------------------------------------------------------------------------------
# 5. Build and Launch Containers
# ------------------------------------------------------------------------------
echo "[Step 5/6] Building and launching multi-container services via Docker Compose..."

docker compose down || true
docker compose build
docker compose up -d

# ------------------------------------------------------------------------------
# 6. Service Health Verification
# ------------------------------------------------------------------------------
echo "[Step 6/6] Waiting for services to initialize and verify health..."
sleep 15

docker compose ps

echo ""
echo "=================================================="
echo "Deployment Complete!"
echo "=================================================="
echo "Airflow Web UI:        http://<EC2-PUBLIC-IP>:8080"
echo "Streamlit Dashboard:   http://<EC2-PUBLIC-IP>:8501"
echo ""
echo "To monitor container logs in real time:"
echo "  docker compose logs -f"
echo "=================================================="
