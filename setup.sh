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

run_tests() {
    echo "Running unit tests..."
    make test-unit

    echo "Running end-to-end tests..."
    make test-e2e
}

set_airflow_variables() {
    echo "Setting Airflow variables..."
    
    # Check if .env file exists
    if [ ! -f .env ]; then
        echo ".env file not found. Please create a .env file with DUNE_API_KEY and DUNE_QUERY_ID."
        exit 1
    fi

    # Read variables from .env file
    source .env

    # Check if variables are set
    if [ -z "$DUNE_API_KEY" ] || [ -z "$DUNE_QUERY_ID" ]; then
        echo "DUNE_API_KEY or DUNE_QUERY_ID is not set in .env file."
        exit 1
    fi

    # Set Airflow variables
    docker-compose run --rm webserver airflow variables set DUNE_API_KEY "$DUNE_API_KEY"
    docker-compose run --rm webserver airflow variables set DUNE_QUERY_ID "$DUNE_QUERY_ID"
    echo "Airflow variables set successfully."
}

main() {
    # check_docker
    # check_docker_compose

    echo "Setting up CoW Swap Price Improvement Analysis..."

    # Build Docker images
    echo "Building Docker images..."
    make build

    # Run tests
    # echo "Running tests..."
    echo "Skipping tests..."
    # run_tests

    # Initialize Airflow
    echo "Initializing Airflow..."
    make init

    # Set Airflow variables
    set_airflow_variables

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