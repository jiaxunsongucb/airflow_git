from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.roofstock_plugin import RoofstockKubernetesPodOperator
from airflow.macros.roofstock_plugin import pod_xcom_pull, default_affinity
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator


year = 2017


default_args = {
    'owner': 'jsong',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('acs_ingest', default_args=default_args, schedule_interval=timedelta(minutes=100))

code_folder = "ACS"

# --------------------------------------------------------
# Transfer data from FTP to S3

docs_FTP_to_S3 = RoofstockKubernetesPodOperator(dag=dag, task_id="docs_FTP_to_S3", code_folder=code_folder)
template_FTP_to_S3 = RoofstockKubernetesPodOperator(dag=dag, task_id="template_FTP_to_S3", code_folder=code_folder)


def subdag_transfer_sequence(parent_dag_name, child_dag_name, default_args):
    dag_subdag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=default_args
    )

    state_list = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware',
                  'DistrictOfColumbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa',
                  'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota',
                  'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'NewHampshire', 'NewJersey', 'NewMexico',
                  'NewYork', 'NorthCarolina', 'NorthDakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'PuertoRico',
                  'RhodeIsland', 'SouthCarolina', 'SouthDakota', 'Tennessee', 'Texas', 'UnitedStates', 'Utah',
                  'Vermont', 'Virginia', 'Washington', 'WestVirginia', 'Wisconsin', 'Wyoming']

    for state in state_list:
        RoofstockKubernetesPodOperator(
            code_folder=code_folder,
            script_name="acs_ingest",
            python_callable="sequence_FTP_to_S3",
            task_id=f"{child_dag_name}-State-{state}",
            dag=dag_subdag,
            wait_for_downstream=True,
            provide_context=True,
            env_vars={"year": year, "state": state}
        )

    return dag_subdag


sequence_FTP_to_S3 = SubDagOperator(dag=dag,
                                    task_id="sequence_FTP_to_S3",
                                    subdag=subdag_transfer_sequence('acs_ingest', 'sequence_FTP_to_S3', default_args))

# --------------------------------------------------------
# Populate database

copy_geo_S3_to_Snowflake = RoofstockKubernetesPodOperator(dag=dag, task_id="copy_geo_S3_to_Snowflake", code_folder=code_folder)
copy_lookup_S3_to_Snowflake = RoofstockKubernetesPodOperator(dag=dag, task_id="copy_lookup_S3_to_Snowflake", code_folder=code_folder)


def subdag_copy_sequence(parent_dag_name, child_dag_name, default_args):
    dag_subdag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=default_args
        )

    for sequence in range(1, 150):
        RoofstockKubernetesPodOperator(
            code_folder=code_folder,
            script_name="acs_ingest",
            python_callable="copy_sequence_S3_to_Snowflake",
            task_id=f"{child_dag_name}-Seq{sequence}",
            dag=dag_subdag,
            wait_for_downstream=True,
            provide_context=True,
            env_vars={"year": year, "sequence": sequence}
            )

    return dag_subdag


copy_sequence_S3_to_Snowflake = SubDagOperator(dag=dag,
                                               task_id="copy_sequence_S3_to_Snowflake",
                                               subdag=subdag_copy_sequence('acs_ingest', 'copy_sequence_S3_to_Snowflake', default_args))

# --------------------------------------------------------
# Update VARIABLE_LISTS and FACT tables
update_geometa = RoofstockKubernetesPodOperator(dag=dag, task_id="update_geometa", code_folder=code_folder)


def subdag_update_fact_on_Snowflake(parent_dag_name, child_dag_name, default_args):
    dag_subdag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=default_args
    )

    variable_list_to_S3 = RoofstockKubernetesPodOperator(
        code_folder=code_folder,
        script_name="acs_ingest",
        task_id="upload_variable_list_to_S3",
        dag=dag_subdag,
        wait_for_downstream=True,
        provide_context=True
    )

    variable_list_to_Snowflake = RoofstockKubernetesPodOperator(
        code_folder=code_folder,
        script_name="acs_ingest",
        task_id="upload_variable_list_to_Snowflake",
        dag=dag_subdag,
        wait_for_downstream=True,
        provide_context=True,
        env_vars={"year": year}
    )

    create_fact_table_on_Snowflake = RoofstockKubernetesPodOperator(
        code_folder=code_folder,
        script_name="acs_ingest",
        task_id="create_fact_table",
        dag=dag_subdag,
        wait_for_downstream=True,
        provide_context=True
    )

    variables_from_raw_tables = RoofstockKubernetesPodOperator(
        code_folder=code_folder,
        script_name="acs_ingest",
        task_id="pull_variables_from_raw_tables",
        dag=dag_subdag,
        wait_for_downstream=True,
        provide_context=True,
        env_vars={"year": year}
    )

    variable_list_to_S3 >> variable_list_to_Snowflake >> create_fact_table_on_Snowflake >> variables_from_raw_tables
    return dag_subdag


update_fact = SubDagOperator(dag=dag,
                             task_id="update_fact",
                             subdag=subdag_update_fact_on_Snowflake('acs_ingest', 'update_fact', default_args))

delete_zips = DummyOperator(
    task_id="delete_zips",
    dag=dag)

# --------------------------------------------------------
# Build graph
docs_FTP_to_S3 >> copy_geo_S3_to_Snowflake >> update_geometa
template_FTP_to_S3 >> copy_lookup_S3_to_Snowflake >> copy_sequence_S3_to_Snowflake
sequence_FTP_to_S3 >> copy_sequence_S3_to_Snowflake >> delete_zips
update_fact.set_upstream([update_geometa, copy_lookup_S3_to_Snowflake, copy_sequence_S3_to_Snowflake])
