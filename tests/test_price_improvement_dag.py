import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
from airflow.models import DagBag
from dags.price_improvement_dag import (
    get_cow_swap_data,
    get_baseline_prices,
    calculate_price_improvement,
)


class TestPriceImprovementDag(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag(dag_folder="dags", include_examples=False)
        self.dag = self.dagbag.get_dag(dag_id="cow_swap_price_improvement")

    def test_dag_loaded(self):
        self.assertIsNotNone(self.dag)
        self.assertEqual(len(self.dag.tasks), 3)

    @patch("dags.price_improvement_dag.DuneClient")
    def test_get_cow_swap_data(self, mock_dune_client):
        mock_dune_client.return_value.run_query_dataframe.return_value = MagicMock()
        result = get_cow_swap_data(execution_date=datetime.now())
        self.assertIsNotNone(result)

    @patch("dags.price_improvement_dag.requests.get")
    def test_get_baseline_prices(self, mock_get):
        mock_response = MagicMock()
        mock_response.json.return_value = {"prices": [[1625097600000, 2000]]}
        mock_get.return_value = mock_response
        result = get_baseline_prices(execution_date=datetime.now())
        self.assertIsNotNone(result)

    @patch("dags.price_improvement_dag.pd.read_sql")
    def test_calculate_price_improvement(self, mock_read_sql):
        mock_read_sql.side_effect = [
            MagicMock(),  # cow_data
            MagicMock(),  # baseline_prices
        ]
        result = calculate_price_improvement(execution_date=datetime.now())
        self.assertIsNotNone(result)


if __name__ == "__main__":
    unittest.main()
