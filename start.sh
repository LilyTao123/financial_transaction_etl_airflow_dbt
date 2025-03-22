#!/bin/bash

# Exit immediately if any command fails
set -e

echo "🚀 Starting Airflow & dbt environment..."

# Navigate to project directory
cd "$(dirname "$0")"

# Ensure Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Installing now..."
    sudo apt update -y
    sudo apt install -y docker.io docker-compose
    sudo systemctl enable docker
    sudo systemctl start docker
    sudo usermod -aG docker $USER
    echo "✅ Docker installed successfully. Please log out and back in for group changes to take effect."
    exit 1
fi

# Export Google Cloud credentials for BigQuery
export GOOGLE_APPLICATION_CREDENTIALS="$(pwd)/gcp-key.json"

# Start Docker Compose (Airflow + dbt)
echo "🐳 Starting Docker containers..."
docker-compose up -d

# Check running containers
echo "📦 Running containers:"
docker ps

echo "✅ Airflow is running at http://$(hostname -I | awk '{print $1}'):8080"
echo "📜 To check logs, use: docker-compose logs -f"
echo "📌 To stop everything, use: ./stop.sh"
