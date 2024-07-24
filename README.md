# CoW Swap Price Improvement Analysis

This project sets up an Airflow environment to analyze the price improvement of CoW Swap compared to the average market.



## Prerequisites

- Docker
- Docker Compose
- Make

## Quick Start

To set up and run the entire project, simply execute:
```sh
chmod +x setup.sh
./setup.sh
```

This script will:
1. Check if Docker and Docker Compose are installed
2. Build the necessary Docker images
3. Initialize Airflow
4. Start the Airflow services
5. Display the logs

Once the setup is complete, you can access the Airflow web interface at http://localhost:8080 with the following credentials:
- Username: admin
- Password: admin

## Manual Setup

If you prefer to run the commands manually, you can use the following Make commands:

- `make build`: Build the Docker images
- `make init`: Initialize Airflow
- `make up`: Start Airflow services
- `make down`: Stop Airflow services
- `make logs`: View logs
- `make shell`: Access the Airflow shell

## Project Structure

- `dags/price_improvement_dag.py`: The main Airflow DAG file
- `dbt_project/` DBT config example
- `Dockerfile`: Defines the Docker image for Airflow
- `docker-compose.yml`: Defines the services (Airflow, PostgreSQL)
- `requirements.txt`: Lists the Python dependencies
- `Makefile`: Contains shortcuts for common commands
- `setup.sh`: Script to automate the entire setup process

## Customization

To modify the analysis or add new features:
1. Edit the `dags/price_improvement_dag.py` file
2. Rebuild the Docker images using `make build`
3. Restart the services using `make down` followed by `make up`

## Testing

This project includes both unit tests and end-to-end tests for the Airflow DAG.

To run all tests:
```Makefile
make test
```

To run only unit tests:
```Makefile
make test-unit
```

To run only end-to-end tests:
```
make test-e2e   
```

Tests are automatically run during the setup process. If you make changes to the DAG, make sure to run the tests again to ensure everything is working correctly.

## Troubleshooting

If you encounter any issues:
1. Check the logs using `make logs`
2. Ensure all required ports are available (8080 for Airflow webserver)
3. Try stopping all services with `make down`, then start again with `make up`
4. Create dags, logs and plugins folder inside the project directory
```bash
mkdir ./dags ./logs ./plugins
```
5. Set user permissions for Airflow to your current user
```bash
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env
```



## Airflow
http://localhost:8080
http://localhost:8080/health

## PSQL
```bash
psql -h your_machine_ip -p 5432 -U airflow -d airflow
```
