# -*- coding: utf-8 -*-

# These are commonly used utilities by Airflow DAGs

###########################################################
# Packages
###########################################################
from airflow.models import Variable, XCom
from datetime import datetime
from functools import wraps
import io
import logging
from pathlib import Path
import pandas as pd
import time
import traceback
import os
import requests
import sys
import json

# This is the class you derive to create a plugin
from airflow.plugins_manager import AirflowPlugin

# Importing base classes that we need to derive
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

import pytz

from airflow import configuration as conf
from airflow.hooks.S3_hook import S3Hook
from airflow.utils import timezone
from airflow.contrib.kubernetes.pod import Resources
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
import snowflake.connector


###########################################################
# Global helper functions
###########################################################
def pd_read_s3(bucket, key, s3_client, *args, **kwargs):
    """
    Function
    ----------------------
    Read csv/excel file living on S3.

    Parameter
    ----------------------
    bucket : string
        S3 bucket name

    key : string
        S3 file path, which is called "key" on S3

    s3_client : boto3.client("s3")

    Return
    ----------------------
    pandas DataFrame object

    """
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    if key[-4:] == ".csv":
        return pd.read_csv(io.BytesIO(obj["Body"].read()), encoding="ISO-8859-1", *args, **kwargs)
    else:
        try:
            return pd.read_excel(io.BytesIO(obj["Body"].read()), encoding="ISO-8859-1", *args, **kwargs)
        except:
            raise Exception("The file is not a csv or an excel file!")


def connect_to_snowflake(database, schema):
    conn_config = {
        "user": Variable.get("SNOWFLAKE_USER"),
        "password": Variable.get("SNOWFLAKE_PASSWORD"),
        "schema": schema,
        "database": database,
        "account": Variable.get("SNOWFLAKE_ACCOUNT"),
        "warehouse": Variable.get("SNOWFLAKE_WH")
    }
    con = snowflake.connector.connect(**conn_config)
    return con


def connect_to_s3():
    AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY").replace("/", "%2F")
    os.environ["AIRFLOW_CONN_AWS_DEFAULT"] = f's3://{AWS_ACCESS_KEY_ID}:{AWS_SECRET_ACCESS_KEY}@s3'
    s3hook = S3Hook()
    return s3hook


def pod_xcom_push(xcom: dict):
    print(f"Pushing XCom {xcom}...")
    dag_id = os.getenv("dag_id", "test_dag")
    task_id = os.getenv("task_id", "test_task")
    execution_date = timezone.utcnow()
    xcom_obj = XCom()
    for key in xcom:
        xcom_obj.set(key, xcom[key], execution_date, task_id, dag_id)


def pod_xcom_pull(dag_id, task_id, key):
    print(f"Pulling XCom from DAG {dag_id}, task {task_id}, key {key}...")
    execution_date = timezone.utcnow()
    xcom_obj = XCom()
    result = xcom_obj.get_one(execution_date, key, task_id, dag_id, include_prior_dates=True)
    return result


def create_logger(tz="UTC"):
    """
    Function
    ----------------------
    Create logging object and return the object and log file path.

    Parameter
    ----------------------
    None

    Return
    ----------------------
    logger : logging object
    """
    # get system environments
    DAG_ID = os.getenv("dag_id", "test_dag")
    TASK_ID = os.getenv("task_id", "test_task")
    EXECUTION_DATETIME = f"{datetime.now():%Y-%m-%dT%H:%M:%S.%f}"

    # create logger
    logger = logging.getLogger(f"{DAG_ID}-{TASK_ID}-{EXECUTION_DATETIME}")
    # all messages will be printed on the console and added into airflow default log files
    logger.setLevel(logging.INFO)

    # create file handler
    BASE_LOG_FOLDER = conf.get('core', 'BASE_LOG_FOLDER')
    log_folder = f"{BASE_LOG_FOLDER}/{DAG_ID}/{TASK_ID}"
    if not os.path.isdir(log_folder):
        os.makedirs(log_folder)
    log_path = f"{log_folder}/{DAG_ID}-{TASK_ID}-{EXECUTION_DATETIME}.log"
    fh = logging.FileHandler(log_path, mode='a')
    fh.setLevel(logging.INFO)

    # create formatter
    fmt = '%(asctime)s {%(module)s:%(lineno)d} %(levelname)s - %(message)s'
    datefmt = f'%Y-%m-%dT%H:%M:%S {tz}'

    def _customTime(*args):
        utc_dt = pytz.utc.localize(datetime.utcnow())
        my_tz = pytz.timezone(tz)
        converted = utc_dt.astimezone(my_tz)
        return converted.timetuple()

    logging.Formatter.converter = _customTime
    formatter = logging.Formatter(fmt, datefmt)

    # add handler and formatter to logger
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # send message to stdout
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    # do not send message to parent logs
    logger.propagate = False

    logger.info(f"Start logging - DAG name: {DAG_ID}, task id: {TASK_ID}.")
    return logger


class LoggerWriter(object):
    """
    Allows to redirect stdout and stderr to logger
    """

    def __init__(self, logger, level):
        """
        :param logger: The log object
        :param level: The log level method to write to, ie. log.debug, log.warning
        :return:
        """
        self.logger = logger
        self.level = level
        self._buffer = str()

    def write(self, message):
        """
        Do whatever it takes to actually log the specified logging record
        :param message: message to log
        """
        if not message.endswith("\n"):
            self._buffer += message
        else:
            self._buffer += message
            self.logger.log(self.level, self._buffer.rstrip())
            self._buffer = str()

    def flush(self):
        """
        Ensure all logging output has been flushed
        """
        if len(self._buffer) > 0:
            self.logger.log(self.level, self._buffer)
            self._buffer = str()


def run_with_logging(tz="US/Pacific"):
    """
    Function
    ----------------------
    Logging wrapper

    Parameter
    ----------------------
    None

    Return
    ----------------------
    Decorated function
    """

    def decorate(func):

        @wraps(func)
        def wrapper(**kwargs):
            # mute snowflake.connector logging
            # workaround for this issue:
            # https://support.snowflake.net/s/question/0D50Z00008gYUIGSA4/python-snowflakeconnectornetwork-error-when-logging-enabled
            logging.getLogger("snowflake.connector").propagate = False

            logger = create_logger(tz)

            # save all stdout to the logger
            sys.stdout = LoggerWriter(logger, logging.INFO)
            try:
                result = func(logger, **kwargs)
                logger.info("Process finished with exit code 0")
                logger.removeHandler(logger.handlers[0])
                return result

            except Exception as error:
                logger.error(error)
                logger.error("\n" + "".join(traceback.format_exception(*sys.exc_info())))
                logger.error("Process finished with exit code 1")
                logger.removeHandler(logger.handlers[0])
                sys.exit(1)

        return wrapper

    return decorate


def send_slack_message(dag_id, task_id, dag_owner, task_status, msg_body, send_log=False):
    """
    Function
    ----------------------
    Send Slack message with status of Airflow DAG
        https://api.slack.com/docs/messages
        https://roofstock-inc.slack.com/services/BBTU106L9? --> alert-airflow Slack channel
    Note: please try to limit the number of messages your DAG sends to Slack:
        e.g. only send a message on DAG failure and/or success, etc.

    Parameter
    ----------------------
    task_id : string

    dag_id : string

    dag_owner : string

    task_status : string
        "SUCCEEDED" or "FAILED"

    msg_body : string
        The message of customized log

    send_log : boolean
        indicates whether to send system log file or not

    Return
    ----------------------
    None

    """
    # Prep the Slack message
    color_dic = {"FAILED": "danger",
                 "SUCCEEDED": "good"}
    color = color_dic.get(task_status, "#EAECEE")  # if task_status is not in the dic, use grey

    # load slack id mapper from a json file.
    slack_id_mapper = json.load(open("/airflow/utilities/slack_id_mapper.json", "r"))
    slack_id = slack_id_mapper.get(dag_owner, None)
    if slack_id:
        owner_line = f"Owner: *{dag_owner}* <@{slack_id}>"
    else:
        owner_line = f"Owner: *{dag_owner}*"

    url = Variable.get('SLACK_WEBHOOK')  # the URL of the webhook that we post Slack messages to
    dag_link = f"http://roofstock-dataeng-airflow.westus.cloudapp.azure.com:8080/admin/airflow/graph?dag_id={dag_id}"
    msg_title = f"DAG: *<{dag_link}|{dag_id}>* Task: *{task_id}* just *{task_status}*!\n{owner_line}"

    payload = {"username": "Airflow Bot Notifications",
               "text": msg_title,
               "attachments": [{"title": "Customized log",
                                "text": msg_body,
                                "color": color
                                }
                               ]
               }

    # Optionally attach log file
    if send_log:
        # attach log files (airflow default log file)
        time.sleep(2)  # wait until the log files have been saved
        log = 'No log file found!'
        try:
            p = Path('/home/roofstock/airflow/logs', dag_id, task_id)
            log = sorted([f for f in sorted([d for d in p.iterdir()])[-1].iterdir()])[-1].open().read()
        except:
            pass

        payload["attachments"].append({"title": "System log", "text": log, "color": color})

    # Post the message to the Slack webhook
    r = requests.post(url, json=payload)


def send_message(send_log=True, success_ignore=True, test=False):
    """
    Function
    ----------------------
    Send Slack message with status of Airflow DAG
        https://api.slack.com/docs/messages
        https://roofstock-inc.slack.com/services/BBTU106L9? --> alert-airflow Slack channel
    Note: please try to limit the number of messages your DAG sends to Slack:
        e.g. only send a message on DAG failure and/or success, etc.

    Parameter
    ----------------------
    task_id : string

    dag_id : string

    dag_owner : string

    task_status : string
        "SUCCEEDED" or "FAILED"

    msg_body : string
        The message of customized log

    send_log : boolean
        indicates whether to send system log file or not

    Return
    ----------------------
    None

    """

    def decorate(func):
        @wraps(func)
        def wrapper(**kwargs):
            # Initialize variables
            task_id = kwargs["task"].task_id
            dag_owner = kwargs["task"].owner
            dag_id = kwargs["dag"].dag_id

            # Start off the slack message that contains the summary of the run
            msg_body = f"START Logging dag: {dag_id}, task: {task_id}\nUTC: {datetime.now():%Y-%m-%dT%H:%M:%S}\n"
            msg_body += "-" * 80 + "\n"
            global logger
            logger, log_path = create_logger(task_id, dag_id)

            try:
                result = func(logger, **kwargs)

            except Exception as e:
                logger.error("\n" + "".join(traceback.format_exception(*sys.exc_info())))
                msg_body += open(log_path, "r").read()
                msg_body += "Failed!"
                if not test:
                    send_slack_message(dag_id, task_id, dag_owner, "FAILED", msg_body, send_log=send_log)
                raise  # reraises the exception
            else:
                msg_body += open(log_path, "r").read()
                msg_body += "Succeeded!"
                if not test and not success_ignore:
                    send_slack_message(dag_id, task_id, dag_owner, "SUCCEEDED", msg_body, send_log=False)
            return result

        return wrapper

    return decorate


def default_affinity():
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
    return affinity

###########################################################
# Airflow Plugin Operators
###########################################################

class BashMessageOperator(BashOperator):
    """
    BashMessageOperator object that inherits BashOperator.
    """

    def __init__(self, success_ignore=True, send_log=False, test=False, *args, **kwargs):
        super(BashMessageOperator, self).__init__(*args, **kwargs)
        self.send_log = send_log
        self.success_ignore = success_ignore
        self.test = test

    def execute(self, context):
        """
        Call the execute in the super class,
        parse the log file and then send slack message.
        """
        from pathlib import Path
        import re
        from airflow.exceptions import AirflowException

        try:
            result = super(BashMessageOperator, self).execute(context)
        except:
            pass

        returncode = self.sp.returncode
        dag_id = self.dag_id
        task_id = self.task_id
        dag_owner = self.owner

        p = Path("/home/roofstock/airflow/logs", dag_id, task_id)
        log = sorted([f for f in sorted([d for d in p.iterdir()])[-1].iterdir()])[-1].open()

        msg_body = ""
        start = False
        end = False
        for line in log:
            if re.findall(r'{bash_operator.py:\d*} INFO - Command exited', line):
                end = True
            if start and not end and re.findall(r'.*{bash_operator.py:\d*}.*- ', line):
                line = re.findall(r'.*{bash_operator.py:\d*}.*- (.*)', line)[0]
                line = re.sub(r'(:?\x1b\[\d*m|\x1b\[0m)', '', line)
                msg_body += line + "\n"
            if re.findall(r'{bash_operator.py:\d*} INFO - Output:', line):
                start = True

        if returncode:
            task_status = "FAILED"
            if not self.test:
                send_slack_message(dag_id, task_id, dag_owner, task_status, msg_body, send_log=self.send_log)
            raise AirflowException("Bash command failed!")
        elif "WARNING" in msg_body.upper():
            task_status = "FAILED"
            if not self.test:
                send_slack_message(dag_id, task_id, dag_owner, task_status, msg_body, send_log=self.send_log)
            raise AirflowException("Found WARNING!")
        elif "ERROR" in msg_body.upper() and "ERROR=0" not in msg_body:
            task_status = "FAILED"
            if not self.test:
                send_slack_message(dag_id, task_id, dag_owner, task_status, msg_body, send_log=self.send_log)
            raise AirflowException("Found ERROR!")
        else:
            task_status = "SUCCEEDED"
            if not self.success_ignore:
                send_slack_message(dag_id, task_id, dag_owner, task_status, msg_body, send_log=False)
            return result


class RoofstockKubernetesPodOperator(KubernetesPodOperator):
    """
    Airflow Plugin KubernetesPodOperator Class that inherits default KubernetesPodOperator.
    """

    def __init__(self,
                 code_folder,
                 script_name=str(),
                 python_callable=str(),
                 python_kwargs={},
                 image=str(),
                 namespace=str(),
                 cmds=[],
                 arguments=[],
                 labels={},
                 startup_timeout_seconds=0,
                 name=str(),
                 env_vars={},
                 volume_mounts=[],
                 volumes=[],
                 secrets=[],
                 in_cluster=True,
                 cluster_context=None,
                 get_logs=True,
                 image_pull_policy="Always",
                 node_selectors={},
                 annotations={},
                 affinity={},
                 do_xcom_push=False,
                 resources=None,
                 config_file=None,
                 image_pull_secrets=None,
                 service_account_name="default",
                 is_delete_operator_pod=True,
                 hostnetwork=True,
                 tolerations=[],
                 configmaps=[],
                 security_context={},
                 *args,
                 **kwargs):
        # required from user: dag, task_id, code_folder, python_callable
        super(KubernetesPodOperator, self).__init__(*args, **kwargs)
        self.script_name = script_name or self.dag_id
        self.python_callable = python_callable or self.task_id
        self.python_kwargs = python_kwargs
        self.image = image or "jiaxun/datatools:general_purpose"
        self.namespace = namespace or conf.get("kubernetes", "namespace")
        self.code_folder = code_folder
        self.cmds = cmds or ["bash", "-cx", "--"]
        self.arguments = arguments or self.default_arguments
        self.labels = labels or {self.dag_id: self.task_id}
        self.startup_timeout_seconds = startup_timeout_seconds or 300
        self.name = name or self.default_name
        self.env_vars = {**env_vars, **self.default_env_vars}
        self.volume_mounts = volume_mounts or self.default_volumes_and_volume_mounts[0]
        self.volumes = volumes or self.default_volumes_and_volume_mounts[1]
        self.secrets = secrets
        self.in_cluster = in_cluster
        self.cluster_context = cluster_context
        self.get_logs = get_logs
        self.image_pull_policy = image_pull_policy
        self.node_selectors = node_selectors
        self.annotations = annotations
        self.affinity = affinity or self.default_affinity
        self.do_xcom_push = self.xcom_push = do_xcom_push
        self.resources = resources or self.default_resources
        self.config_file = config_file
        self.image_pull_secrets = image_pull_secrets
        self.service_account_name = service_account_name
        self.is_delete_operator_pod = is_delete_operator_pod
        self.hostnetwork = hostnetwork
        self.tolerations = tolerations
        self.configmaps = configmaps
        self.security_context = security_context
        self.executor_config = self.default_executor_config

    @staticmethod
    def _volume_factory(name, claimName, mount_path, sub_path, read_only=True, persistentVolumeClaim=True):
        if persistentVolumeClaim:
            volume_config = {
                'persistentVolumeClaim':
                    {
                        'claimName': claimName
                    }
            }
        else:
            volume_config = {
                'configMap':
                    {
                        'name': claimName,
                        'defaultMode': 420
                    }
            }

        volume = Volume(name=name, configs=volume_config)

        volume_mount = VolumeMount(name=name,
                                   mount_path=mount_path,
                                   sub_path=sub_path,
                                   read_only=read_only)

        return volume, volume_mount

    @property
    def default_name(self):
        # a DNS-1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end
        # with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[
        # a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')
        return (self.script_name + "-" + self.python_callable).replace("_", "-")

    @property
    def default_arguments(self):
        return [(f'cd /root/airflow/code/dags/code_for_kubernetes_pod_operator/{self.code_folder} && '
                 f'python -c "from {self.script_name} import *; {self.python_callable}(**{self.python_kwargs})"')]

    @property
    def default_env_vars(self):
        default_env_vars = dict(AIRFLOW__CORE__EXECUTOR="LocalExecutor",
                                dag_id=self.dag_id,
                                task_id=self.task_id)
        return default_env_vars

    @property
    def default_volumes_and_volume_mounts(self):
        env_name = self.namespace.replace("airflow-", "")
        airflow_code_volume, airflow_code_volume_mount = self._volume_factory("airflow-code",
                                                                              "airflow-code-claim",
                                                                              "/root/airflow/code",
                                                                              f"{env_name}/code",
                                                                              True)
        airflow_logs_volume, airflow_logs_volume_mount = self._volume_factory("airflow-logs",
                                                                              "airflow-logs-claim",
                                                                              "/root/airflow/logs",
                                                                              f"{env_name}/logs",
                                                                              False)
        airflow_config_volume, airflow_config_volume_mount = self._volume_factory("airflow-config",
                                                                                  "airflow-configmap",
                                                                                  "/root/airflow/airflow.cfg",
                                                                                  "airflow.cfg",
                                                                                  True,
                                                                                  False)

        return ([airflow_code_volume_mount, airflow_logs_volume_mount, airflow_config_volume_mount],
                [airflow_code_volume, airflow_logs_volume, airflow_config_volume])

    @property
    def default_affinity(self):
        return default_affinity()

    @property
    def default_resources(self):
        return Resources(request_memory="128Mi", request_cpu="300m", limit_memory="1024Mi", limit_cpu="500m")

    @property
    def default_executor_config(self):
        return {"KubernetesExecutor": {"affinity": self.default_affinity}}


###########################################################
# Defining the plugin class
###########################################################

class AirflowPlugin(AirflowPlugin):
    # The name of your plugin (str)
    name = "roofstock_plugin"
    # A list of class(es) derived from BaseOperator
    operators = [BashMessageOperator, RoofstockKubernetesPodOperator]
    # A list of class(es) derived from BaseHook
    hooks = []
    # A list of class(es) derived from BaseExecutor
    executors = []
    # A list of references to inject into the macros namespace
    macros = [send_slack_message, create_logger, pd_read_s3, send_message, run_with_logging,
              connect_to_s3, connect_to_snowflake, pod_xcom_push, pod_xcom_pull, default_affinity]
    # A list of objects created from a class derived
    # from flask_admin.BaseView
    admin_views = []
    # A list of Blueprint object created from flask.Blueprint
    flask_blueprints = []
    # A list of menu links (flask_admin.base.MenuLink)
    menu_links = []
