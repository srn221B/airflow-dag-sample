from google.cloud import bigquery
from google.oauth2 import service_account
import json
from datetime import timedelta
from airflow import DAG, settings
from airflow.models import Connection
from airflow.operators.python_operator import PythonOperator
from airflow_dag_sample.operators.slack_api_post_operator import SlackAPIPostOperatorHatiware
from airflow_dag_sample.operators.bigquery_get_data_operator import BigQueryGetDataOperator
from airflow.utils.dates import days_ago




default_args = {
    'owner': 'shimoyama',
    'depends_on_past': False,
    'start_date': days_ago(2),
}
dag = DAG(
    'test_slack',
    default_args=default_args,
    description='For TEST execute DAG',
    schedule_interval=timedelta(days=1),
    tags=["work"]
)

def big_query(ti,**kwargs):
    key_path = '/var/local/google_cloud_default.json'
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    query = (
        "SELECT cast(export_time as date) AS t1 ,project.name,service.description,sum(cost) AS t2 \
        FROM `annular-surf-336608.dataset.gcp_billing_export_v1_01D2A8_A178BF_6CF5C8` \
        WHERE cast(export_time as date) = DATE_SUB(CURRENT_DATE('Asia/Tokyo'), INTERVAL 1 DAY) \
        group by cast(export_time as date),project.name,service.description;")

    #client = bigquery.Client()
    client = bigquery.Client(
        credentials=credentials,
        project=credentials.project_id,
    )
    query_job = client.query(query)
    rows = query_job.result()
    total_cost=0
    text=f"昨日のGCP使用料金"
    print(rows)
    for row in rows:
        total_cost += int(row.t2)
        text=text+f"\n{row.name}.{row.description}   ¥{row.t2}"
    if total_cost > 1000:
        text=text+"\n\nお買い物検定3級"
    else:
        text=text+"\n\nお買い物検定2級"
    return text

big_query = PythonOperator(
    dag=dag,
    provide_context=True, 
    task_id='big_query',
    python_callable=big_query,
)

slack = SlackAPIPostOperatorHatiware(
    task_id="post_hello",
    text="{{ ti.xcom_pull('big_query')}}",
    dag=dag,
    channel="costs"
)


big_query >> slack