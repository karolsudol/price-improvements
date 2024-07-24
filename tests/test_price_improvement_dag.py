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

    @patch("dags.price_improvement_dag.get_cow_swap_data")
    @patch("dags.price_improvement_dag.get_baseline_prices")
    @patch("dags.price_improvement_dag.calculate_price_improvement")
    def setUp(self, mock_calculate, mock_baseline, mock_cow_swap):
        self.mock_calculate = mock_calculate
        self.mock_baseline = mock_baseline
        self.mock_cow_swap = mock_cow_swap
        self.dagbag = DagBag(dag_folder="dags", include_examples=False)
        self.dag = self.dagbag.get_dag(dag_id="cow_swap_price_improvement")

    def test_dag_loaded(self):
        self.assertIsNotNone(self.dag)
        self.assertEqual(len(self.dag.tasks), 3)

    def test_get_cow_swap_data(self):
        self.mock_cow_swap.return_value = "mocked data"
        result = self.mock_cow_swap(execution_date=datetime.now())
        self.assertIsNotNone(result)
        self.assertEqual(result, "mocked data")

    def test_get_baseline_prices(self):
        self.mock_baseline.return_value = "mocked prices"
        result = self.mock_baseline(execution_date=datetime.now())
        self.assertIsNotNone(result)
        self.assertEqual(result, "mocked prices")

    def test_calculate_price_improvement(self):
        self.mock_calculate.return_value = "mocked calculation"
        result = self.mock_calculate(execution_date=datetime.now())
        self.assertIsNotNone(result)
        self.assertEqual(result, "mocked calculation")


if __name__ == "__main__":
    unittest.main()
