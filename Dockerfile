FROM apache/airflow:2.9.3

USER root

# Install PostgreSQL client
RUN apt-get update && apt-get install -y postgresql-client

# Create the airflow group
RUN groupadd -g 5000 airflow

# Create and set permissions for the log directory
RUN mkdir -p /opt/airflow/logs
RUN chown -R 5000:5000 /opt/airflow/logs
RUN chmod -R 755 /opt/airflow/logs

USER airflow

# Install additional Python packages
COPY requirements.txt /requirements.txt
RUN pip install --user --no-cache-dir -r /requirements.txt

# Copy DAG file
COPY dags/price_improvement_dag.py /opt/airflow/dags/price_improvement_dag.py