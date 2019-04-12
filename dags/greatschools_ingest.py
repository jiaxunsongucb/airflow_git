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
attachment_to_s3 = RoofstockKubernetesPodOperator(dag=dag, task_id="attachment_to_s3", code_folder=code_folder)
staging_to_s3 = RoofstockKubernetesPodOperator(dag=dag, task_id="staging_to_s3", code_folder=code_folder)
copy_to_snowflake = RoofstockKubernetesPodOperator(dag=dag, task_id="copy_to_snowflake", code_folder=code_folder)


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
    dag=dag,
    executor_config={"KubernetesExecutor": {"affinity": default_affinity()}})

skipped = DummyOperator(
    task_id="skipped",
    dag=dag)

dbt_config_volume, dbt_config_volume_mount = volume_factory(name="dbt-test-profile",
                                                            claimName="dbt-test-profile",
                                                            mount_path="/root/.dbt/profiles.yml",
                                                            sub_path="profiles_test1.yml",
                                                            read_only=True,
                                                            persistentVolumeClaim=False)

dbt_test_pass = RoofstockKubernetesPodOperator(dag=dag,
                                               task_id="dbt_test_pass",
                                               python_callable="dbt_test",
                                               code_folder=code_folder,
                                               volumes=[dbt_config_volume],
                                               volume_mounts=[dbt_config_volume_mount],
                                               python_kwargs={"model_name": "DBT_TEST"})

dbt_test_fail = RoofstockKubernetesPodOperator(dag=dag,
                                               task_id="dbt_test_fail",
                                               python_callable="dbt_test",
                                               code_folder=code_folder,
                                               volumes=[dbt_config_volume],
                                               volume_mounts=[dbt_config_volume_mount],
                                               python_kwargs={"model_name": "DO_NOT_EXIST"})

dbt_test_pass >> dbt_test_fail >> attachment_to_s3 >> branching >> staging_to_s3 >> copy_to_snowflake
branching >> skipped
