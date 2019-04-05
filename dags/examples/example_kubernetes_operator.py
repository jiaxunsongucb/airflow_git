from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.pod import Resources
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.operators.dummy_operator import DummyOperator

affinity = {
    'nodeAffinity': {
        'requiredDuringSchedulingIgnoredDuringExecution': {
            'nodeSelectorTerms': [ 
                {
                    'matchExpressions': [ 
                        {
                            'key': 'k8s-core-server', 
                            'operator': 'NotIn', 
                            'values': ['true']
                        },
                        {
                            'key': 'airflow-server', 
                            'operator': 'In', 
                            'values': ['false']
                        }
                    ]
                }
            ]
        }
    }
}

resources = Resources(request_memory="256Mi", request_cpu="300m", limit_memory="256Mi", limit_cpu="300m")

def _volume_factory(name, claimName, mount_path, sub_path, read_only=True, persistentVolumeClaim=True):
    if persistentVolumeClaim:
        volume_config= {
            'persistentVolumeClaim':
              {
                'claimName': claimName
              }
            }
    else:
        volume_config= {
            'configMap':
              {
                'name': claimName,
                'defaultMode': 420
              }
            }        

    volume = Volume(name=name, configs=volume_config)
    
    volume_mount = VolumeMount( name=name,
                                mount_path=mount_path,
                                sub_path=sub_path,
                                read_only=read_only)
    
    return volume, volume_mount


default_args = {
    'owner': 'jsong',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('example_kubernetes_operator', default_args=default_args, schedule_interval=timedelta(minutes=100))

start = DummyOperator(task_id='run_this_first', dag=dag)

argument_passing = """
airflow run example_kubernetes_executor_using_pod_operator one_task $(date +%Y-%m-%dT%H:%M:%S) --local --subdir DAGS_FOLDER/code_for_kubernetes_pod_operator/example_kubernetes_executor.py --ignore_all_dependencies
"""

argument_failing = """
airflow run example_kubernetes_executor_using_pod_operator two_task $(date +%Y-%m-%dT%H:%M:%S) --local --subdir DAGS_FOLDER/code_for_kubernetes_pod_operator/example_kubernetes_executor.py --ignore_all_dependencies --interactive
"""

airflow_dags_volume, airflow_dags_volume_mount = _volume_factory("airflow-dags", "airflow-dags-claim", "/root/airflow/dags", "jsong/dags", True)
airflow_logs_volume, airflow_logs_volume_mount = _volume_factory("airflow-logs", "airflow-logs-claim", "/root/airflow/logs", "jsong/logs", False)
airflow_config_volume, airflow_config_volume_mount = _volume_factory("airflow-config", "airflow-configmap", "/root/airflow/airflow.cfg", "airflow.cfg", True, False)

env_vars = {"AIRFLOW__CORE__EXECUTOR": "LocalExecutor"}

passing = KubernetesPodOperator(namespace='airflow-jsong',
                                image="jiaxun/datatools:airflow",
                                cmds=["bash","-cx","--"],
                                arguments=[argument_passing],
                                labels={"foo": "bar"},
                                name="passing-test",
                                task_id="passing-task",
                                get_logs=True,
                                dag=dag,
                                in_cluster=True,
                                is_delete_operator_pod=False,
                                hostnetwork=True,
                                startup_timeout_seconds=180,
                                affinity=affinity,
                                resources=resources,
                                volumes=[airflow_dags_volume, airflow_logs_volume, airflow_config_volume], # volumes with s!!!!
                                volume_mounts=[airflow_dags_volume_mount, airflow_logs_volume_mount, airflow_config_volume_mount],
                                env_vars=env_vars,
                                executor_config={"KubernetesExecutor": {"image": "jiaxun/datatools:airflow", 
                                                                        "affinity": affinity}}
                               )

failing = KubernetesPodOperator(namespace='airflow-jsong',
                                image="jiaxun/datatools:airflow",
                                cmds=["bash","-cx","--"],
                                arguments=[argument_failing],
                                labels={"foo": "bar"},
                                name="failing-test",
                                task_id="failing-task",
                                get_logs=True,
                                dag=dag,
                                in_cluster=True,
                                is_delete_operator_pod=False,
                                hostnetwork=True,
                                startup_timeout_seconds=180,
                                affinity=affinity,
                                resources=resources,
                                volumes=[airflow_dags_volume, airflow_logs_volume, airflow_config_volume], # volumes with s!!!!
                                volume_mounts=[airflow_dags_volume_mount, airflow_logs_volume_mount, airflow_config_volume_mount],
                                env_vars=env_vars,
                                executor_config={"KubernetesExecutor": {"image": "jiaxun/datatools:airflow", 
                                                                        "affinity": affinity}}
                               )

failing_two = KubernetesPodOperator(namespace='airflow-jsong',
                          image="ubuntu:latest",
                          cmds=["Python","-c"],
                          arguments=["print('hello world')"],
                          labels={"foo": "bar"},
                          name="failing-test-two",
                          task_id="failing-task-two",
                          get_logs=True,
                          dag=dag,
                          in_cluster=True,
                          is_delete_operator_pod=False,
                          affinity=affinity,
                          resources=resources,
                          executor_config={"KubernetesExecutor": {"image": "jiaxun/datatools:airflow", "affinity": affinity}}
                               )

passing.set_upstream(start)
failing.set_upstream(start)
failing_two.set_upstream(start)