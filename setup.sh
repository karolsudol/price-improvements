#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Function to check if Docker is installed
check_docker() {
    if ! command -v docker &> /dev/null; then
        echo "Docker is not installed. Please install Docker and try again."
        exit 1
    fi
}

# Function to check if Docker Compose is installed
check_docker_compose() {
    if ! command -v docker-compose &> /dev/null; then
        echo "Docker Compose is not installed. Please install Docker Compose and try again."
        exit 1
    fi
}

# Main setup function
main() {
    check_docker
    check_docker_compose

    echo "Setting up CoW Swap Price Improvement Analysis..."

    # Build Docker images
    echo "Building Docker images..."
    make build

    # Initialize Airflow
    echo "Initializing Airflow..."
    make init

    # Start Airflow
    echo "Starting Airflow..."
    make up

    echo "Setup complete! Airflow is now running."
    echo "You can access the Airflow web interface at http://localhost:8080"
    echo "Username: admin"
    echo "Password: admin"
    
    # Display logs
    echo "Displaying logs. Press Ctrl+C to exit."
    make logs
}

# Run the main function
main