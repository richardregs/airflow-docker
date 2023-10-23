from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import  DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator

default_args = {
    'depends_on_past': False
}

CLUSTER_NAME = 'cluster-proyecto-bigdata-24'
REGION = 'us-central1'
PROJECT_ID = 'proyecto-big-data-24'
PYSPARK_URI_1 = 'gs://dmc-proyecto-big-data-24/code/sparkTask/taskfirst.py'
PYSPARK_URI_2 = 'gs://dmc-proyecto-big-data-24/code/sparkTask/tasksecond.py'
PYSPARK_URI_3 = 'gs://dmc-proyecto-big-data-24/code/sparkTask/taskfirst.py'
PYSPARK_URI_4 = 'gs://dmc-proyecto-big-data-24/code/sparkTask/tasksecond.py'


PYSPARK_JOB_1 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI_1},
}

PYSPARK_JOB_2 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI_2},
}

PYSPARK_JOB_3 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI_3},
}

PYSPARK_JOB_4 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI_4},
}

with DAG(
    'dataproc-dmc',
    default_args=default_args,
    description='A simple DAG to create a Dataproc Workflow',
    schedule_interval="@monthly",
    start_date=days_ago(1)
) as dag:

    submit_job_1 = DataprocSubmitJobOperator(
        task_id="Workload",
        job=PYSPARK_JOB_1,
        region=REGION,
        project_id=PROJECT_ID,
        dag=dag
    )

    submit_job_2 = DataprocSubmitJobOperator(
        task_id="Landing",
        job=PYSPARK_JOB_2,
        region=REGION,
        project_id=PROJECT_ID,
        dag=dag
    )

    submit_job_3 = DataprocSubmitJobOperator(
        task_id="Curated",
        job=PYSPARK_JOB_3,
        region=REGION,
        project_id=PROJECT_ID,
        dag=dag
    )

    submit_job_4 = DataprocSubmitJobOperator(
        task_id="Functional",
        job=PYSPARK_JOB_4,
        region=REGION,
        project_id=PROJECT_ID,
        dag=dag
    )

    submit_job_1 >> submit_job_2 >> submit_job_3 >> submit_job_4
