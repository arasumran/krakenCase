import requests
import json
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago


headers = {"Content-Type": "application/json"}
COMPUTATION_BASE_URL = "http://krakencase_computation_instance_1:8000/"

def download_data(**kwargs):
    params = {"url_path":kwargs['url_path'],'file_name':kwargs['file_name']}
    res = requests.post(url = kwargs['url'],params=params,headers=headers)
    if res.status_code != 200:
        raise ValueError("Task failed with error message {}".format(json.loads(res.content)))
    return True

def transformation(**kwargs):
    params = {"outages_file_name":kwargs['outages_file_name'],
                                            'site_id_file_name':kwargs['site_id_file_name']}
    res = requests.post(url =kwargs['url'],params=params,headers=headers)
    print(json.loads(res.content))
    if res.status_code != 200:
        raise ValueError("Task failed with error message {}".format(json.loads(res.content)))
    return True

def send_request_for_next_step(**kwargs):
    res = requests.post(url=kwargs['url'],headers=headers)
    print(json.loads(res.content))
    if res.status_code != 200:
        raise ValueError("Task failed with error message {}".format(json.loads(res.content)))
    return True


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG(dag_id="kraken_outages_site_id_mapping",
         default_args=default_args,
         schedule_interval=None) as dag:
    with TaskGroup("ingest_data_with_APIs") as task_group:
        download_outages = PythonOperator(task_id="download_outages",
                                          python_callable=download_data,
                                          op_kwargs={'url': COMPUTATION_BASE_URL + 'download_data',
                                                     'url_path':'outages',
                                                     'file_name': 'outages.json'},
                                          provide_context=True,
                                          retries=2)

        download_site_ids = PythonOperator(task_id="download_site_ids",
                                           python_callable=download_data,
                                           op_kwargs={'url': COMPUTATION_BASE_URL + 'download_data',
                                           'url_path':'site-info/norwich-pear-tree',
                                           'file_name': 'site_ids.json'},
                                           provide_context=True,
                                           retries=2)

    map_outages_with_site_ids = PythonOperator(task_id="map_outages_with_site_ids",
                                               op_kwargs={'url':  COMPUTATION_BASE_URL + 'transform_data',
                                                          'outages_file_name': 'outages.json',
                                                          'site_id_file_name': 'site_ids.json'},
                                                provide_context=True,
                                               python_callable=transformation)

    send_site_outages = PythonOperator(task_id="send_site_outages",
                                       op_kwargs = {'url':  COMPUTATION_BASE_URL + 'send_outaged_site_ids'},
                                      provide_context=True,
                                       python_callable=send_request_for_next_step)

    clear_downloaded_context = PythonOperator(task_id="clear_downloaded_context",
                                              op_kwargs={'url':  COMPUTATION_BASE_URL + 'delete_files'},
                                                provide_context=True,
                                              python_callable=send_request_for_next_step)

    [download_outages, download_site_ids] >> map_outages_with_site_ids >> send_site_outages >> clear_downloaded_context