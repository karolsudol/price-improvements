import pandas as pd
import os
from dotenv import load_dotenv
import requests
from datetime import datetime, timedelta
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase

try:
    load_dotenv()
    DUNE_API_KEY = os.getenv("DUNE_API_KEY")
except FileNotFoundError:
    print("Error: .env file not found")

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    print("Airflow not available. Running in local mode.")


def get_cow_swap_data(**kwargs):
    execution_date = kwargs["execution_date"]
    start_date = execution_date.date() - timedelta(days=1)
    end_date = execution_date.date()

    query = QueryBase(
        name="CoW Swap USDC-WETH Trades",
        query_id=1215383,  # Replace with the actual query ID
        params=[
            QueryParameter.date_type(
                name="StartDate",
                value=datetime.combine(start_date, datetime.min.time()).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            ),
            QueryParameter.date_type(
                name="EndDate",
                value=datetime.combine(end_date, datetime.min.time()).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            ),
            QueryParameter.text_type(
                name="BuyTokenAddress",
                value="0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            ),
            QueryParameter.text_type(
                name="SellTokenAddress",
                value="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            ),
        ],
    )
    dune = DuneClient(api_key=DUNE_API_KEY)
    results_df = dune.run_query_dataframe(query)

    # Store results in PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    results_df.to_sql(
        "cow_swap_data",
        pg_hook.get_sqlalchemy_engine(),
        if_exists="replace",
        index=False,
    )


# Function to get baseline prices from CoinGecko
def get_baseline_prices(**kwargs):
    execution_date = kwargs["execution_date"]
    start_date = execution_date.date() - timedelta(days=1)
    end_date = execution_date.date()

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


# Function to calculate price improvement
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
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
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
    get_cow_swap_data(execution_date=datetime.now())
    get_baseline_prices(execution_date=datetime.now())
    calculate_price_improvement(execution_date=datetime.now())
