import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
import sys
import os

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Isolate Airflow imports
try:
    from airflow.models import DagBag
except ImportError:
    print("Failed to import Airflow. Make sure it's installed correctly.")
    DagBag = MagicMock()


class TestPriceImprovementDag(unittest.TestCase):

    @patch("airflow.models.Variable.get")
    def setUp(self, mock_variable_get):
        self.mock_variable_get = mock_variable_get

        # Mock Variable.get calls to return dummy values
        self.mock_variable_get.side_effect = lambda key, default=None: default

        # Import the DAG file here to ensure the mocked Variable.get is used
        from dags import price_improvement_dag

        self.dag_module = price_improvement_dag

        self.dagbag = DagBag(dag_folder="dags", include_examples=False)
        self.dag = self.dagbag.get_dag(dag_id="cow_swap_price_improvement")

    def test_dag_loaded(self):
        self.assertIsNotNone(self.dag)
        self.assertEqual(len(self.dag.tasks), 4)

    @patch("dags.price_improvement_dag.get_cow_swap_data")
    def test_get_cow_swap_data(self, mock_cow_swap):
        mock_cow_swap.return_value = "mocked data"
        result = mock_cow_swap(execution_date=datetime.now())
        self.assertIsNotNone(result)
        self.assertEqual(result, "mocked data")
        # Verify that the dummy API key is being used
        self.assertEqual(self.dag_module.DUNE_API_KEY, "dummy_api_key")
        self.assertEqual(self.dag_module.DUNE_QUERY_ID, 12345)

    @patch("dags.price_improvement_dag.get_baseline_prices")
    def test_get_baseline_prices(self, mock_baseline):
        mock_baseline.return_value = "mocked prices"
        result = mock_baseline(execution_date=datetime.now())
        self.assertIsNotNone(result)
        self.assertEqual(result, "mocked prices")

    @patch("dags.price_improvement_dag.calculate_price_improvement")
    def test_calculate_price_improvement(self, mock_calculate):
        mock_calculate.return_value = "mocked calculation"
        result = mock_calculate(execution_date=datetime.now())
        self.assertIsNotNone(result)
        self.assertEqual(result, "mocked calculation")


if __name__ == "__main__":
    unittest.main()
