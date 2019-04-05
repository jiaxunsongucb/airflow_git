from __future__ import print_function
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
import os
import time
from datetime import datetime, timedelta
import requests, zipfile, io

args = {
    'owner': 'jsong',
    'start_date': datetime(2019, 3, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='example_kubernetes_executor', 
    default_args=args,
    schedule_interval=timedelta(minutes=10)
)
"""
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
          - labelSelector:
              matchExpressions:
              - key: component
                operator: In
                values:
                - tunnel
            topologyKey: "kubernetes.io/hostname"
            namespaces: [kube-system]
"""
affinity1 = {
    'podAntiAffinity': {
        'requiredDuringSchedulingIgnoredDuringExecution': [
            {   'namespaces' : ['kube-system'],
                'topologyKey': 'kubernetes.io/hostname',
                'labelSelector': {
                    'matchExpressions': [
                        {
                            'key': 'component',
                            'operator': 'In',
                            'values': ['tunnel']
                        }
                    ]
                }
            }
        ]
    }
}
"""
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions:
          - key: kubernetes.io/e2e-az-name
            operator: In
            values:
            - e2e-az1
            - e2e-az2
"""
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


executor_config = {
        "KubernetesExecutor": {"request_memory": "128Mi",
                               "limit_memory": "128Mi",
#                                "tolerations": tolerations,
                               "affinity": affinity
                              }}

tolerations = [{
    'key': 'dedicated',
    'operator': 'Equal',
    'value': 'airflow'
}]


def print_stuff():
    print("stuff!")


def use_zip_binary():
    rc = os.system("zip")
    assert rc == 0

def heavy_task():
    i = 0
    while True:
        print (i)
        time.sleep(5)
        i += 1

def net_test():
    print ("net_test!!!")
    # Download the zip files from www.census.gov
    def _download_zips(year):
        if year == 2010:
            url = "http://www2.census.gov/geo/docs/maps-data/data/gazetteer/Gaz_tracts_national.zip"
        else:
            url = f"http://www2.census.gov/geo/docs/maps-data/data/gazetteer/{year}_Gazetteer/{year}_Gaz_tracts_national.zip"
        
        print ("before requests")
        r = requests.get(url)
        data = io.BytesIO(r.content)
        print ("after requests")
        with zipfile.ZipFile(data, mode='r') as zipf:
            for file in zipf.infolist():
                fileName = file.filename
                print (fileName)
                with open(f"{year}_Gaz_tracts_national.txt", "wb") as f:
                    f.write(zipf.read(file))
    
    for year in [2010] + [y for y in range(2012, 2019)]:
        print(year)
        _download_zips(year)

# You don't have to use any special KubernetesExecutor configuration if you don't want to
start_task = PythonOperator(
    task_id="start_task", python_callable=print_stuff, dag=dag,
    executor_config={"KubernetesExecutor": {"image": "jiaxun/datatools:airflow_zip", 
                                            "request_memory": "256Mi",
                                            "limit_memory": "256Mi",
                                            "request_cpu": "200m",
                                            "limit_cpu": "200m",
                                            "affinity": affinity}}
)

# But you can if you want to
one_task = PythonOperator(
    task_id="one_task", python_callable=net_test, dag=dag,
    executor_config={"KubernetesExecutor": {"image": "jiaxun/datatools:airflow",                            
                                            "request_memory": "256Mi",
                                            "request_cpu": "200m",
                                            "affinity": affinity}}
)

# Use the zip binary, which is only found in this special docker image
two_task = PythonOperator(
    task_id="two_task", python_callable=use_zip_binary, dag=dag,
    executor_config={"KubernetesExecutor": {"image": "jiaxun/datatools:airflow_zip", 
                                            "request_memory": "256Mi",
                                            "limit_memory": "256Mi",
                                            "request_cpu": "200m",
                                            "limit_cpu": "200m",
                                            "affinity": affinity}}
)

two_task_fail = PythonOperator(
    task_id="two_task_fail", python_callable=use_zip_binary, dag=dag,
    executor_config={"KubernetesExecutor": {"image": "jiaxun/datatools:airflow", 
                                            "request_memory": "256Mi",
                                            "limit_memory": "256Mi",
                                            "request_cpu": "200m",
                                            "limit_cpu": "200m",
                                            "affinity": affinity}}
)

# Limit resources on this operator/task with node affinity & tolerations
three_task = PythonOperator(
    task_id="three_task", python_callable=print_stuff, dag=dag,
    executor_config={
        "KubernetesExecutor": {"request_memory": "256Mi",
                               "limit_memory": "256Mi",
                               "request_cpu": "200m",
                               "limit_cpu": "200m",
#                                "tolerations": tolerations,
                               "affinity": affinity
                              }}
)

start_task.set_downstream([one_task, two_task, two_task_fail, three_task])
