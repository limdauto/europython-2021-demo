from collections import defaultdict

from pathlib import Path
from typing import List

from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.version import version
from datetime import datetime, timedelta

from kedro.framework.session import KedroSession
from kedro.framework.project import configure_project


class KedroOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        package_name: str,
        pipeline_name: str,
        node_name: str,
        project_path: str,
        env: str,
        from_inputs: List[str] = None,
        to_outputs: List[str] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.package_name = package_name
        self.pipeline_name = pipeline_name
        self.node_name = node_name
        self.project_path = project_path
        self.env = env
        self.from_inputs = from_inputs
        self.to_outputs = to_outputs

    def execute(self, context):
        configure_project(self.package_name)
        with KedroSession.create(
            self.package_name, self.project_path, env=self.env
        ) as session:
            session.run(
                self.pipeline_name,
                from_inputs=self.from_inputs,
                to_outputs=self.to_outputs,
            )


# Kedro settings required to run your pipeline
env = "local"
pipeline_name = "__default__"
project_path = Path.cwd()
package_name = "europython_2021_demo"

# Default settings applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG(
    "europython-2021-demo-grouped-nodes",
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=timedelta(
        minutes=30
    ),  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
    default_args=default_args,
    catchup=False,  # enable if you don't want historical dag runs to run
) as dag:

    tasks = {}

    tasks["data_processing"] = KedroOperator(
        task_id="data-processing",
        package_name=package_name,
        pipeline_name=pipeline_name,
        node_name="data-processing",
        project_path=project_path,
        env=env,
        to_outputs=["rated_movies"],
    )

    tasks["model_training"] = KedroOperator(
        task_id="model-training",
        package_name=package_name,
        pipeline_name=pipeline_name,
        node_name="model-training",
        project_path=project_path,
        env=env,
        from_inputs=["rated_movies"],
    )

    tasks["data_processing"] >> tasks["model_training"]
