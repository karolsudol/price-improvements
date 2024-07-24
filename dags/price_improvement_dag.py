import pandas as pd
import requests
from datetime import datetime, timedelta
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

DUNE_API_KEY = Variable.get("DUNE_API_KEY")
DUNE_QUERY_ID = int(Variable.get("DUNE_QUERY_ID"))

START_DATE = datetime(2024, 7, 1)
END_DATE = datetime(2024, 7, 1)

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    print("Airflow not available. Running in local mode.")


def check_postgres_connection():
    try:
        conn = BaseHook.get_connection("postgres_default")
        print(f"Connection {conn.conn_id} is valid.")
    except AirflowException as e:
        print(f"Connection error: {str(e)}")
        raise


def get_cow_swap_data(**kwargs):

    query = QueryBase(
        name="CoW Swap USDC-WETH Trades",
        query_id=DUNE_QUERY_ID,
        # params=[
        #     QueryParameter.date_type(
        #         name="StartDate",
        #         value=START_DATE.strftime("%Y-%m-%d %H:%M:%S"),
        #     ),
        #     QueryParameter.date_type(
        #         name="EndDate",
        #         value=END_DATE.strftime("%Y-%m-%d %H:%M:%S"),
        #     ),
        # ],
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
    # Define START_DATE and END_DATE from kwargs if needed
    execution_date = kwargs["execution_date"]
    start_date = datetime.combine(
        execution_date.date(), datetime.min.time()
    ) - timedelta(days=1)
    end_date = datetime.combine(execution_date.date(), datetime.min.time())

    # API request URL with correct timestamps
    url = f"https://api.coingecko.com/api/v3/coins/ethereum/market_chart/range?vs_currency=usd&from={int(start_date.timestamp())}&to={int(end_date.timestamp())}"
    response = requests.get(url)
    data = response.json()

    # Create DataFrame
    prices = pd.DataFrame(data["prices"], columns=["timestamp", "price"])
    prices["date"] = pd.to_datetime(prices["timestamp"], unit="ms").dt.date
    prices = prices.set_index("date")

    # Limit to only 10 records
    prices = prices.head(10)

    # Ensure that DataFrame contains data
    if prices.empty:
        raise ValueError(
            "The prices DataFrame is empty. No data was fetched from CoinGecko."
        )

    # Store results in PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    with pg_hook.get_sqlalchemy_engine().connect() as conn:
        prices.to_sql(
            "baseline_prices",
            conn,
            if_exists="replace",
            index=True,  # Keep the date as index
            index_label="date",  # Specify index label to match table schema
        )

    print(f"Stored {len(prices)} rows in the 'baseline_prices' table.")


def calculate_price_improvement(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")

    # Fetch data from PostgreSQL
    cow_data = pd.read_sql(
        "SELECT * FROM cow_swap_data", pg_hook.get_sqlalchemy_engine()
    )
    baseline_prices = pd.read_sql(
        "SELECT date, price FROM baseline_prices",
        pg_hook.get_sqlalchemy_engine(),
        parse_dates=["date"],
    )

    # Prepare cow_data DataFrame
    cow_data["date"] = pd.to_datetime(cow_data["block_time"]).dt.date

    # Join cow_data with baseline_prices on the 'date' column
    cow_data = cow_data.merge(baseline_prices, on="date", how="left")

    # Debug: Print the first few rows of the merged DataFrame
    print("Cow Data after Merging with Baseline Prices:")
    print(cow_data.head())

    # Calculate additional columns
    cow_data["baseline_buy_value"] = (
        cow_data["atoms_sold"] * cow_data["price"] / 1e6
    )  # Assuming USDC has 6 decimals
    cow_data["price_improvement"] = (
        cow_data["buy_value_usd"] - cow_data["baseline_buy_value"]
    )

    # Debug: Print the first few rows after calculations
    print("Cow Data after Calculations:")
    print(cow_data.head())

    # Store results in PostgreSQL
    with pg_hook.get_sqlalchemy_engine().connect() as conn:
        # Drop the table if it exists
        conn.execute("DROP TABLE IF EXISTS price_improvement")

        # Create a new table with the updated data
        cow_data.to_sql(
            "price_improvement",
            pg_hook.get_sqlalchemy_engine(),
            if_exists="replace",  # 'replace' will drop the table if it exists
            index=False,  # Do not write row indices
        )

    # Output some statistics for verification
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

# Add this for local testing
if __name__ == "__main__":
    # This will allow you to run the script locally for testing
    check_connection()
    get_baseline_prices()
    get_cow_swap_data()
    calculate_price_improvement()
