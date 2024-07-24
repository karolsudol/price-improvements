import pandas as pd
import requests
from datetime import datetime, timedelta
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from airflow.models import Variable

DUNE_API_KEY = Variable.get("DUNE_API_KEY")
DUNE_QUERY_ID = int(Variable.get("DUNE_QUERY_ID"))

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    print("Airflow not available. Running in local mode.")


def get_cow_swap_data(**kwargs):
    # Fixed start and end dates
    start_date = datetime(2024, 7, 1)
    end_date = datetime(2024, 7, 1)

    query = QueryBase(
        name="CoW Swap USDC-WETH Trades",
        query_id=DUNE_QUERY_ID,
        params=[
            QueryParameter.date_type(
                name="StartDate",
                value=start_date.strftime("%Y-%m-%d %H:%M:%S"),
            ),
            QueryParameter.date_type(
                name="EndDate",
                value=end_date.strftime("%Y-%m-%d %H:%M:%S"),
            ),
        ],
    )
    dune = DuneClient(
        api_key=DUNE_API_KEY, request_timeout=600
    )  # Set timeout to 10 minutes

    results_df = dune.run_query_dataframe(query)

    # Store results in PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    results_df.to_sql(
        "cow_swap_data",
        pg_hook.get_sqlalchemy_engine(),
        if_exists="replace",
        index=False,
    )


def get_baseline_prices(**kwargs):
    # Fixed start and end dates
    start_date = datetime(2024, 7, 1)
    end_date = datetime(2024, 7, 1)

    url = f"https://api.coingecko.com/api/v3/coins/ethereum/market_chart/range?vs_currency=usd&from={int(start_date.timestamp())}&to={int(end_date.timestamp())}"
    response = requests.get(url)
    data = response.json()
    prices = pd.DataFrame(data["prices"], columns=["timestamp", "price"])
    prices["date"] = pd.to_datetime(prices["timestamp"], unit="ms").dt.date
    prices = prices.set_index("date")["price"]

    # Store results in PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    prices.to_sql(
        "baseline_prices", pg_hook.get_sqlalchemy_engine(), if_exists="replace"
    )


def calculate_price_improvement(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")

    # Fetch data from PostgreSQL
    cow_data = pd.read_sql(
        "SELECT * FROM cow_swap_data", pg_hook.get_sqlalchemy_engine()
    )
    baseline_prices = pd.read_sql(
        "SELECT * FROM baseline_prices",
        pg_hook.get_sqlalchemy_engine(),
        index_col="date",
    )

    cow_data["date"] = pd.to_datetime(cow_data["block_time"]).dt.date
    cow_data["baseline_price"] = cow_data["date"].map(baseline_prices["price"])
    cow_data["baseline_buy_value"] = (
        cow_data["atoms_sold"] * cow_data["baseline_price"] / 1e6
    )  # Assuming USDC has 6 decimals
    cow_data["price_improvement"] = (
        cow_data["buy_value_usd"] - cow_data["baseline_buy_value"]
    )

    # Store results in PostgreSQL
    cow_data.to_sql(
        "price_improvement",
        pg_hook.get_sqlalchemy_engine(),
        if_exists="replace",
        index=False,
    )

    print(f"Average price improvement: {cow_data['price_improvement'].mean()} USD")
    print(f"Total trades analyzed: {len(cow_data)}")


if AIRFLOW_AVAILABLE:
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2024, 7, 24),
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),  # Fixed import issue
    }

    dag = DAG(
        "cow_swap_price_improvement",
        default_args=default_args,
        description="A DAG to analyze CoW Swap price improvement",
        schedule_interval="0 9 * * *",  # Run daily at 9 AM UTC
        catchup=False,
    )

    dune_task = PythonOperator(
        task_id="get_cow_swap_data",
        python_callable=get_cow_swap_data,
        provide_context=True,
        dag=dag,
    )

    coingecko_task = PythonOperator(
        task_id="get_baseline_prices",
        python_callable=get_baseline_prices,
        provide_context=True,
        dag=dag,
    )

    analysis_task = PythonOperator(
        task_id="calculate_price_improvement",
        python_callable=calculate_price_improvement,
        provide_context=True,
        dag=dag,
    )

    dune_task >> coingecko_task >> analysis_task

else:
    print("Airflow not available. DAG not created.")

# Add this for local testing
if __name__ == "__main__":
    # This will allow you to run the script locally for testing
    get_cow_swap_data()
    get_baseline_prices()
    calculate_price_improvement()
