from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.roofstock_plugin import RoofstockKubernetesPodOperator
from airflow.macros.roofstock_plugin import pod_xcom_pull, default_affinity
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

default_args = {
    'owner': 'jsong',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('acs_ingest', default_args=default_args, schedule_interval=timedelta(minutes=100))

code_folder = "ACS"
docs_FTP_to_S3 = RoofstockKubernetesPodOperator(dag=dag, task_id="docs_FTP_to_S3", code_folder=code_folder)
template_FTP_to_S3 = RoofstockKubernetesPodOperator(dag=dag, task_id="template_FTP_to_S3", code_folder=code_folder)
sequence_FTP_to_S3 = RoofstockKubernetesPodOperator(dag=dag, task_id="sequence_FTP_to_S3", code_folder=code_folder)
copy_geo_S3_to_Snowflake = RoofstockKubernetesPodOperator(dag=dag, task_id="copy_geo_S3_to_Snowflake", code_folder=code_folder)
copy_lookup_S3_to_Snowflake = RoofstockKubernetesPodOperator(dag=dag, task_id="copy_lookup_S3_to_Snowflake", code_folder=code_folder)
copy_sequence_S3_to_Snowflake = RoofstockKubernetesPodOperator(dag=dag, task_id="copy_sequence_S3_to_Snowflake", code_folder=code_folder)
update_geometa = RoofstockKubernetesPodOperator(dag=dag, task_id="update_geometa", code_folder=code_folder)
update_fact = RoofstockKubernetesPodOperator(dag=dag, task_id="update_fact", code_folder=code_folder)

delete_zips = DummyOperator(
    task_id="delete_zips",
    dag=dag)

# --------------------------------------------------------
# Build graph
docs_FTP_to_S3 >> copy_geo_S3_to_Snowflake >> update_geometa
template_FTP_to_S3 >> copy_lookup_S3_to_Snowflake >> copy_sequence_S3_to_Snowflake
sequence_FTP_to_S3 >> copy_sequence_S3_to_Snowflake >> delete_zips
update_fact.set_upstream([update_geometa, copy_lookup_S3_to_Snowflake, copy_sequence_S3_to_Snowflake])