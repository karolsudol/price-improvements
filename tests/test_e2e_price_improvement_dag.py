import unittest
import os
from airflow import settings
from airflow.models import DagBag
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
import pendulum
from airflow.configuration import conf

# Set Airflow home
os.environ["AIRFLOW_HOME"] = "/home/elcomandante/airflow"

# Ensure the SQLite database connection is set
conf.set("core", "sql_alchemy_conn", "sqlite:////home/elcomandante/airflow/airflow.db")


class TestPriceImprovementDagE2E(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag(dag_folder=settings.DAGS_FOLDER, include_examples=False)
        print(f"Loaded DAGs: {self.dagbag.dag_ids}")  # Debug print
        self.dag = self.dagbag.get_dag(dag_id="cow_swap_price_improvement")
        self.execution_date = pendulum.now().subtract(days=1)

    def test_dag_e2e(self):
        if self.dag is None:
            print("Available DAGs:", self.dagbag.dag_ids)  # Print available DAG IDs
            self.fail("DAG 'cow_swap_price_improvement' not found in DagBag")

        with create_session() as session:
            # Create a DagRun for the execution date
            dag_run = self.dag.create_dagrun(
                state=State.RUNNING,
                execution_date=self.execution_date,
                run_type=DagRunType.MANUAL,
                session=session,
            )

            try:
                # Run the DAG
                self.dag.run(
                    start_date=self.execution_date,
                    end_date=self.execution_date,
                    run_id=dag_run.run_id,
                    executor=None,  # Let Airflow choose the default executor
                    session=session,
                )

                # Refresh the dag_run to get updated task instances
                session.refresh(dag_run)

                # Check the status of tasks
                for task_instance in dag_run.get_task_instances(session=session):
                    self.assertEqual(
                        task_instance.state,
                        State.SUCCESS,
                        f"Task {task_instance.task_id} failed with state {task_instance.state}",
                    )

            except Exception as e:
                self.fail(f"DAG execution failed with error: {str(e)}")

    def tearDown(self):
        # Clean up any resources or data created during the test
        pass


if __name__ == "__main__":
    unittest.main()
