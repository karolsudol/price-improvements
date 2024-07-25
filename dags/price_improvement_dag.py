"""
This module contains an Airflow DAG for analyzing CoW Swap price improvement.
It fetches data from Dune Analytics and CoinGecko, processes it, and stores the results in PostgreSQL.
"""

from datetime import datetime, timedelta
import pandas as pd
import requests
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

DUNE_API_KEY = Variable.get("DUNE_API_KEY")
DUNE_QUERY_ID = int(Variable.get("DUNE_QUERY_ID"))

# Calculate previous day's date in UTC
PREVIOUS_DAY = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
START_DATE = PREVIOUS_DAY
END_DATE = PREVIOUS_DAY

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    print("Airflow not available. Running in local mode.")


def check_postgres_connection():
    """Check if the PostgreSQL connection is valid."""
    try:
        conn = BaseHook.get_connection("postgres_default")
        print(f"Connection {conn.conn_id} is valid.")
    except AirflowException as e:
        print(f"Connection error: {str(e)}")
        raise


def get_cow_swap_data(**kwargs):
    """Fetch CoW Swap data from Dune Analytics and store it in PostgreSQL."""
    query = QueryBase(
        name="CoW Swap USDC-WETH Trades",
        query_id=DUNE_QUERY_ID,
        params=[
            QueryParameter.text_type(
                name="StartDate",
                value=START_DATE,
            ),
            QueryParameter.text_type(
                name="EndDate",
                value=END_DATE,
            ),
        ],
    )
    dune = DuneClient(api_key=DUNE_API_KEY, request_timeout=600)

    results_df = dune.run_query_dataframe(query)

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    results_df.to_sql(
        "cow_swap_data",
        pg_hook.get_sqlalchemy_engine(),
        if_exists="replace",
        index=False,
    )


def get_baseline_prices(**kwargs):
    """Fetch baseline prices from CoinGecko and store them in PostgreSQL."""
    start_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date = datetime.strptime(END_DATE, "%Y-%m-%d") + timedelta(days=1)

    url = (
        f"https://api.coingecko.com/api/v3/coins/ethereum/market_chart/range"
        f"?vs_currency=usd&from={int(start_date.timestamp())}&to={int(end_date.timestamp())}"
    )
    response = requests.get(url)
    data = response.json()

    prices = pd.DataFrame(data["prices"], columns=["timestamp", "price"])
    prices["date"] = pd.to_datetime(prices["timestamp"], unit="ms").dt.date

    if prices.empty:
        raise ValueError(
            "The prices DataFrame is empty. No data was fetched from CoinGecko."
        )

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    with pg_hook.get_sqlalchemy_engine().connect() as conn:
        prices.to_sql(
            "baseline_prices",
            conn,
            if_exists="replace",
            index=False,
        )

    print(f"Stored {len(prices)} rows in the 'baseline_prices' table.")


def calculate_price_improvement(**kwargs):
    """Calculate price improvement and store results in PostgreSQL."""
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")

    cow_data = pd.read_sql(
        "SELECT * FROM cow_swap_data", pg_hook.get_sqlalchemy_engine()
    )
    baseline_prices = pd.read_sql(
        "SELECT * FROM baseline_prices",
        pg_hook.get_sqlalchemy_engine(),
    )

    cow_data["block_time"] = pd.to_datetime(cow_data["block_time"], utc=True)
    cow_data["timestamp"] = cow_data["block_time"]

    baseline_prices["timestamp"] = pd.to_datetime(
        baseline_prices["timestamp"], unit="ms", utc=True
    )

    cow_data["timestamp"] = cow_data["timestamp"].dt.floor("T")
    baseline_prices["timestamp"] = baseline_prices["timestamp"].dt.floor("T")

    cow_data_agg = (
        cow_data.groupby("timestamp")
        .agg(
            {
                "buy_price": "mean",
                "buy_value_usd": "mean",
                "atoms_sold": "mean",
            }
        )
        .reset_index()
    )

    baseline_prices_agg = (
        baseline_prices.groupby("timestamp").agg({"price": "mean"}).reset_index()
    )

    merged_data = pd.merge(
        baseline_prices_agg, cow_data_agg, on="timestamp", how="inner"
    )

    merged_data["price_improvement"] = (
        merged_data["price"] - merged_data["buy_price"]
    ).round(2)

    with pg_hook.get_sqlalchemy_engine().connect() as conn:
        cow_data_agg.to_sql(
            "cow_data_aggregated",
            conn,
            if_exists="replace",
            index=False,
        )

        baseline_prices_agg.to_sql(
            "baseline_prices_aggregated",
            conn,
            if_exists="replace",
            index=False,
        )

        merged_data[["timestamp", "price_improvement"]].to_sql(
            "price_improvement",
            conn,
            if_exists="replace",
            index=False,
        )

    print(
        f"Average price improvement: {merged_data['price_improvement'].mean():.2f} USD"
    )
    print(f"Total trades analyzed: {len(merged_data)}")


if AIRFLOW_AVAILABLE:
    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": datetime(2024, 7, 24),
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    }

    dag = DAG(
        "cow_swap_price_improvement",
        default_args=default_args,
        description="A DAG to analyze CoW Swap price improvement",
        schedule_interval="0 9 * * *",
        catchup=False,
    )

    check_connection = PythonOperator(
        task_id="check_postgres_connection",
        python_callable=check_postgres_connection,
        dag=dag,
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

    check_connection >> coingecko_task >> dune_task >> analysis_task

else:
    print("Airflow not available. DAG not created.")

if __name__ == "__main__":
    get_baseline_prices()
    get_cow_swap_data()
    calculate_price_improvement()
