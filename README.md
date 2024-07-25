
# CoW Swap Price Improvement Analysis

**Table of Contents**
-------------------
* [TODO](#todo)
* [Prerequisites](#prerequisites)
* [Quick Start](#quick-start)
* [Manual Setup](#manual-setup)
* [Project Structure](#project-structure)
* [Customization](#customization)
* [Testing](#testing)
* [Accessing Fetched Data](#accessing-fetched-data)
* [Troubleshooting](#troubleshooting)
* [License](#license)


### TODO
------
`analysis`: currently not to be used for insights, but rather proves technically correct, 
join was doen on localised utc timestamp but needs more tests to verify the join is correct
also, it needs to be improved join on larger baseline_prices ensuring data consistency

`optimisations`: partition on dates and add indexes depending in the data size vs query performance and costs and fiter by execution date 

`schemas and sql queries`: currently hardcoded, needs to be set up as a module in DBT with versioning

`cloud deployment`: currently on local but both airflow and DB has to be in serverless and scalable environment

`test`: expand units to cover more cases for each function annd task and integrate E2E tests in cloud env.

`data-warehouse`: potentially set up a cloud based data warehouse with normalised tables while isolating compute and storage, spark is an o overkill for this, but executing queries in a some distributed fashion is a must

`real-time` build geth websocket connection app to fetch and insert data from the contracts in real time so then real time analysis is possible

`modules`: set up a python module classes, so that the code can be reused in other projects


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

make sure you set .env with vars:
```
DUNE_API_KEY={YOUR_KEY}
DUNE_QUERY_ID=3940146
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
5. Set user permissions for Airflow to your current user ex: 
```bash
sudo chown -R airflow:airflow /opt/airflow
```
5. If fails, set manually `DUNE_API_KEY` and `DUNE_QUERY_ID` can be done in airflow.cfg or console

### License
-------

MIT License

