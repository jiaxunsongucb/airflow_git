from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.roofstock_plugin import RoofstockKubernetesPodOperator
from airflow.macros.roofstock_plugin import pod_xcom_pull, default_affinity, volume_factory
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'jsong',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('greatschools_ingest', default_args=default_args, schedule_interval=timedelta(minutes=100))

code_folder = "GreatSchools"

attachment_to_s3 = RoofstockKubernetesPodOperator(dag=dag,
                                                  task_id="attachment_to_s3",
                                                  code_folder=code_folder,
                                                  script_name="greatschools_ingest",
                                                  python_callable="attachment_to_s3")

staging_to_s3 = RoofstockKubernetesPodOperator(dag=dag,
                                               task_id="staging_to_s3",
                                               code_folder=code_folder,
                                               script_name="greatschools_ingest",
                                               python_callable="staging_to_s3")

copy_to_snowflake = RoofstockKubernetesPodOperator(dag=dag,
                                                   task_id="copy_to_snowflake",
                                                   code_folder=code_folder,
                                                   script_name="greatschools_ingest",
                                                   python_callable="copy_to_snowflake")


def branching_def(**kwargs):
    stage_location = pod_xcom_pull("greatschools_ingest", "attachment_to_s3", "stage_location")
    print(f"Got stage loaction: {stage_location}")
    if stage_location:
        return "staging_to_s3"
    else:
        return "skipped"


branching = BranchPythonOperator(
    task_id="branching",
    provide_context=True,
    python_callable=branching_def,
    dag=dag)

skipped = DummyOperator(
    task_id="skipped",
    dag=dag)

attachment_to_s3 >> branching >> staging_to_s3 >> copy_to_snowflake
branching >> skipped
