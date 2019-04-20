from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.roofstock_plugin import RoofstockKubernetesPodOperator
from airflow.macros.roofstock_plugin import volume_factory, pod_xcom_pull, default_affinity
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator

year = 2017

default_args = {
    'owner': 'jsong',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('acs_ingest', default_args=default_args, schedule_interval=timedelta(minutes=100))

code_folder = "ACS"

dbt_config_volume, dbt_config_volume_mount = volume_factory(name="dbt-profiles",
                                                            claimName="dbt-profiles",
                                                            mount_path="/root/.dbt/profiles.yml",
                                                            sub_path="profiles_acs.yml",
                                                            read_only=True,
                                                            persistentVolumeClaim=False)

# --------------------------------------------------------
# Transfer data from FTP to S3

docs_FTP_to_S3 = RoofstockKubernetesPodOperator(dag=dag,
                                                task_id="docs_FTP_to_S3",
                                                code_folder=code_folder,
                                                script_name="acs_ingest",
                                                python_callable="docs_FTP_to_S3",
                                                python_kwargs={"year": year})

template_FTP_to_S3 = RoofstockKubernetesPodOperator(dag=dag,
                                                    task_id="template_FTP_to_S3",
                                                    code_folder=code_folder,
                                                    script_name="acs_ingest",
                                                    python_callable="template_FTP_to_S3",
                                                    python_kwargs={"year": year})


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

    for state in ['California']:
        RoofstockKubernetesPodOperator(dag=dag_subdag,
                                       task_id=f"{child_dag_name}-State-{state}",
                                       code_folder=code_folder,
                                       script_name="acs_ingest",
                                       python_callable="sequence_FTP_to_S3",
                                       python_kwargs={"year": year, "state": state})

    return dag_subdag


sequence_FTP_to_S3 = SubDagOperator(dag=dag,
                                    task_id="sequence_FTP_to_S3",
                                    subdag=subdag_transfer_sequence('acs_ingest', 'sequence_FTP_to_S3', default_args),
                                    executor_config={"KubernetesExecutor": {"request_memory": "128Mi",
                                                                            "limit_memory": "1024Mi",
                                                                            "request_cpu": "300m",
                                                                            "limit_cpu": "500m",
                                                                            "affinity": default_affinity()}})

# --------------------------------------------------------
# Populate database

copy_geo_S3_to_Snowflake = RoofstockKubernetesPodOperator(dag=dag,
                                                          task_id="copy_geo_S3_to_Snowflake",
                                                          code_folder=code_folder,
                                                          script_name="acs_ingest",
                                                          python_callable="copy_geo_S3_to_Snowflake",
                                                          python_kwargs={"year": year})

copy_lookup_S3_to_Snowflake = RoofstockKubernetesPodOperator(dag=dag,
                                                             task_id="copy_lookup_S3_to_Snowflake",
                                                             code_folder=code_folder,
                                                             script_name="acs_ingest",
                                                             python_callable="copy_lookup_S3_to_Snowflake",
                                                             python_kwargs={"year": year})


def subdag_copy_sequence(parent_dag_name, child_dag_name, default_args):
    dag_subdag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=default_args
    )

    for sequence in range(1, 150):
        RoofstockKubernetesPodOperator(
            dag=dag_subdag,
            task_id=f"{child_dag_name}-Seq{sequence}",
            code_folder=code_folder,
            script_name="acs_ingest",
            python_callable="copy_sequence_S3_to_Snowflake",
            python_kwargs={"year": year, "sequence": sequence}
        )

    return dag_subdag


copy_sequence_S3_to_Snowflake = SubDagOperator(dag=dag,
                                               task_id="copy_sequence_S3_to_Snowflake",
                                               subdag=subdag_copy_sequence('acs_ingest',
                                                                           'copy_sequence_S3_to_Snowflake',
                                                                           default_args),
                                               executor_config={"KubernetesExecutor": {"request_memory": "128Mi",
                                                                                       "limit_memory": "1024Mi",
                                                                                       "request_cpu": "300m",
                                                                                       "limit_cpu": "500m",
                                                                                       "affinity": default_affinity()}})

# --------------------------------------------------------
# Update VARIABLE_LISTS and FACT tables
update_geometa = RoofstockKubernetesPodOperator(dag=dag,
                                                task_id="update_geometa",
                                                code_folder=code_folder,
                                                script_name="acs_ingest",
                                                python_callable="update_geometa",
                                                volume_mounts=[dbt_config_volume_mount],
                                                volumes=[dbt_config_volume])


def subdag_update_fact_on_Snowflake(parent_dag_name, child_dag_name, default_args):
    dag_subdag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=default_args
    )

    upload_variable_list_to_S3 = RoofstockKubernetesPodOperator(
        dag=dag_subdag,
        task_id="upload_variable_list_to_S3",
        code_folder=code_folder,
        script_name="acs_ingest",
        python_callable="upload_variable_list_to_S3"
    )

    upload_variable_list_to_Snowflake = RoofstockKubernetesPodOperator(
        dag=dag_subdag,
        task_id="upload_variable_list_to_Snowflake",
        code_folder=code_folder,
        script_name="acs_ingest",
        python_callable="upload_variable_list_to_Snowflake",
        python_kwargs={"year": year}
    )

    create_fact_table = RoofstockKubernetesPodOperator(
        dag=dag_subdag,
        task_id="create_fact_table",
        code_folder=code_folder,
        script_name="acs_ingest",
        python_callable="create_fact_table"
    )

    pull_variables_from_raw_tables = RoofstockKubernetesPodOperator(
        dag=dag_subdag,
        task_id="pull_variables_from_raw_tables",
        code_folder=code_folder,
        script_name="acs_ingest",
        python_callable="pull_variables_from_raw_tables",
        python_kwargs={"year": year},
        volume_mounts=[dbt_config_volume_mount],
        volumes=[dbt_config_volume]
    )

    upload_variable_list_to_S3 >> upload_variable_list_to_Snowflake >> create_fact_table >> pull_variables_from_raw_tables
    return dag_subdag


update_fact = SubDagOperator(dag=dag,
                             task_id="update_fact",
                             subdag=subdag_update_fact_on_Snowflake('acs_ingest', 'update_fact', default_args),
                             executor_config={"KubernetesExecutor": {"request_memory": "128Mi",
                                                                     "limit_memory": "1024Mi",
                                                                     "request_cpu": "300m",
                                                                     "limit_cpu": "500m",
                                                                     "affinity": default_affinity()}})

delete_zips = DummyOperator(
    task_id="delete_zips",
    dag=dag)

# --------------------------------------------------------
# Build graph
docs_FTP_to_S3 >> copy_geo_S3_to_Snowflake >> update_geometa
template_FTP_to_S3 >> copy_lookup_S3_to_Snowflake >> copy_sequence_S3_to_Snowflake
sequence_FTP_to_S3 >> copy_sequence_S3_to_Snowflake >> delete_zips
update_fact.set_upstream([update_geometa, copy_lookup_S3_to_Snowflake, copy_sequence_S3_to_Snowflake])
