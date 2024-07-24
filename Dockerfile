FROM apache/airflow:2.7.1

USER root

# Install PostgreSQL client
RUN apt-get update && apt-get install -y postgresql-client

USER airflow

# Install additional Python packages
COPY requirements.txt /requirements.txt
RUN pip install --user --no-cache-dir -r /requirements.txt

# Copy DAG file
COPY src/dags/price_improvement_dag.py /opt/airflow/dags/price_improvement_dag.py