Here is the revised README.md file with a Table of Contents (TOC) that includes links:

# CoW Swap Price Improvement Analysis
======================================================

**Table of Contents**
-------------------

* [Prerequisites](#prerequisites)
* [Quick Start](#quick-start)
* [Manual Setup](#manual-setup)
* [Project Structure](#project-structure)
* [Customization](#customization)
* [Testing](#testing)
* [Accessing Fetched Data](#accessing-fetched-data)
* [Troubleshooting](#troubleshooting)
* [License](#license)
* [TODO](#todo)

### Prerequisites
---------------

* Docker
* Docker Compose
* Make

### Quick Start
-------------

To set up and run the entire project, simply execute:
```
chmod +x setup.sh
./setup.sh
```

### Manual Setup
-------------

If you prefer to run the commands manually, you can use the following Make commands:

* `make build`: Build the Docker images
* `make init`: Initialize Airflow
* `make up`: Start Airflow services
* `make down`: Stop Airflow services
* `make logs`: View logs
* `make shell`: Access the Airflow shell

### Project Structure
-----------------

* `dags/price_improvement_dag.py`: The main Airflow DAG file
* `dbt_project/` DBT project config example - includes the DBT models and seeds
* `Dockerfile`: Defines the Docker image for Airflow
* `docker-compose.yml`: Defines the services (Airflow, PostgreSQL)
* `requirements.txt`: Lists the Python dependencies
* `Makefile`: Contains shortcuts for common commands
* `setup.sh`: Script to automate the entire setup process

### Customization
-------------

To modify the analysis or add new features:

1. Edit the `dags/price_improvement_dag.py` file
2. Rebuild the Docker images using `make build`
3. Restart the services using `make down` followed by `make up`

### Testing
------

This project includes both unit tests and end-to-end tests for the Airflow DAG.

* `make test`: Run all tests
* `make test-unit`: Run only unit tests
* `make test-e2e`: Run only end-to-end tests

### Accessing Fetched Data
---------------------

To access the fetched data:

1. `psql -h localhost -p 5432 -U airflow -d airflow`
2. `SELECT * FROM price_improvement;`
3. `SELECT * FROM baseline_prices;`
4. `SELECT * FROM cow_swap_data;`

### Troubleshooting
-----------------

If you encounter any issues:

1. Check the logs using `make logs`
2. Ensure all required ports are available (8080 for Airflow webserver)
3. Try stopping all services with `make down`, then start again with `make up`
4. Create dags, logs and plugins folder inside the project directory
5. Set user permissions for Airflow to your current user

### License
-------

MIT License

### TODO
------

* fix e2e in actions during merge to main
* model the data tables better
* build better analytics from the results - with dashboards and threshold alerts
* set Airflow in the cloud
* create data warehouse separating storage and compute
* set DBT as query version control tool
* module out the DBT models and seeds
* add schema modules etc