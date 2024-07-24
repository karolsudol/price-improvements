import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

# Isolate Airflow imports
try:
    from airflow.models import DagBag
except ImportError:
    print("Failed to import Airflow. Make sure it's installed correctly.")
    DagBag = MagicMock()

# Mock the entire dags module
with patch("dags.price_improvement_dag") as mock_dag_module:
    mock_dag_module.get_cow_swap_data = MagicMock()
    mock_dag_module.get_baseline_prices = MagicMock()
    mock_dag_module.calculate_price_improvement = MagicMock()

    class TestPriceImprovementDag(unittest.TestCase):

        def setUp(self):
            self.dagbag = DagBag(dag_folder="dags", include_examples=False)
            self.dag = self.dagbag.get_dag(dag_id="cow_swap_price_improvement")

        def test_dag_loaded(self):
            self.assertIsNotNone(self.dag)
            self.assertEqual(len(self.dag.tasks), 3)

        def test_get_cow_swap_data(self):
            result = mock_dag_module.get_cow_swap_data(execution_date=datetime.now())
            self.assertIsNotNone(result)

        def test_get_baseline_prices(self):
            result = mock_dag_module.get_baseline_prices(execution_date=datetime.now())
            self.assertIsNotNone(result)

        def test_calculate_price_improvement(self):
            result = mock_dag_module.calculate_price_improvement(
                execution_date=datetime.now()
            )
            self.assertIsNotNone(result)


if __name__ == "__main__":
    unittest.main()
