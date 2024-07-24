import unittest
from airflow.models import DagBag
from airflow.utils.session import create_session
from airflow.utils.dates import days_ago
from airflow.utils.state import State


class TestPriceImprovementDagE2E(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag(dag_folder="dags", include_examples=False)
        self.dag = self.dagbag.get_dag(dag_id="cow_swap_price_improvement")
        self.execution_date = days_ago(1)

    def test_dag_e2e(self):
        with create_session() as session:
            # Create a DagRun for the execution date
            dag_run = self.dag.create_dagrun(
                state=State.RUNNING,
                execution_date=self.execution_date,
                run_type="manual",
                session=session,
            )

            # Run the DAG
            dag_run.dag.run(
                start_date=self.execution_date,
                end_date=self.execution_date,
                executor=get_default_executor(),
                session=session,
            )

            # Check the status of tasks
            for task_instance in dag_run.get_task_instances():
                self.assertEqual(task_instance.state, State.SUCCESS)


if __name__ == "__main__":
    unittest.main()
