"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

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

default_args = {
    "owner": "jsong",
    "depends_on_past": False,
    "start_date": datetime(2019, 3, 24),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("example_heavy_long_test", default_args=default_args, schedule_interval=timedelta(minutes=10))

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag,
                  executor_config={"KubernetesExecutor": {"image": "jiaxun/datatools:airflow", 
                                            "request_memory": "256Mi",
                                            "limit_memory": "256Mi",
                                            "request_cpu": "200m",
                                            "limit_cpu": "200m",
                                            "affinity": affinity}}
                 )

t2 = BashOperator(task_id="sleep", bash_command="sleep 5", retries=3, dag=dag,
                  executor_config={"KubernetesExecutor": {"image": "jiaxun/datatools:airflow", 
                                            "request_memory": "256Mi",
                                            "limit_memory": "256Mi",
                                            "request_cpu": "200m",
                                            "limit_cpu": "200m",
                                            "affinity": affinity}}
                 )

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t3 = BashOperator(
    task_id="templated",
    bash_command=templated_command,
    params={"my_param": "Parameter I passed in"},
    dag=dag,
    executor_config={"KubernetesExecutor": {"image": "jiaxun/datatools:airflow", 
                                            "request_memory": "256Mi",
                                            "limit_memory": "256Mi",
                                            "request_cpu": "200m",
                                            "limit_cpu": "200m",
                                            "affinity": affinity}}
)

t2.set_upstream(t1)
t3.set_upstream(t1)
