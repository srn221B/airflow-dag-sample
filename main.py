import uuid
import os
from datetime import timedelta
from operator import itemgetter
from airflow import DAG
from airflow.models import Variable
from airflow.macros import ds_add
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow_dag_sample.operators.slack_api_post_operator\
    import SlackAPIPostOperatorHatiware
from airflow_dag_sample.operators.bigquery_operator import BigQueryOperator


default_args = {
    'owner': 'srn221B',
    'depends_on_past': False,
    'start_date': days_ago(2),
}


def get_extract_date(ti):
    return ti.xcom_pull(task_ids='prepare', key='extract_date')


def get_delete_date(ti):
    return ti.xcom_pull(task_ids='prepare', key='delete_date')


def _macros():
    return {
        'bq_table': f"{Variable.get('cost_dataset')}.dataset.{Variable.get('cost_table')}",
        'extract_date': get_extract_date,
        'delete_date': get_delete_date,
    }


with DAG(
    'test_slack',
    default_args=default_args,
    description='日々のGCP使用料をSlackに通知するDAG',
    schedule_interval=timedelta(days=1),
    tags=["work"],
    user_defined_macros=_macros(),
) as dag:
    dag.doc_md = """\
    ## GCPの使用料金をSlackに通知するDAG
    ### 使用するVariable
    - bigquery_sa_keypath
    - slack_token_hatiware
    - cost_dataset
    - cost_table
    """

    def prepare(**kwargs):
        if kwargs['dag_run'].external_trigger \
             and not kwargs['dag_run'].is_backfill:
            extract_date = kwargs['yesterday_ds']
            delete_date = ds_add(kwargs['yesterday_ds'], -5)
        else:
            extract_date = kwargs['ds']
            delete_date = ds_add(kwargs['ds'], -5)
        kwargs['ti'].xcom_push(key='extract_date', value=extract_date)
        kwargs['ti'].xcom_push(key='delete_date', value=delete_date)
        local_log_dir = '/var/log/bigquery/'
        suffix = str(uuid.uuid4()).replace('-', '_')
        kwargs['ti'].xcom_push(
            key='local_log_filepath',
            value=f'{local_log_dir}{extract_date}_{suffix}.csv')

    prepare_task = PythonOperator(
        task_id="prepare",
        python_callable=prepare,
    )

    with TaskGroup(
         "main_task_group", tooltip="bqからextract_dateのgcp利用料金を抽出・加工し、Slackに送る"
         ) as main_group:
        def reader(filepath: str) -> list:
            with open(filepath) as f:
                for row in f:
                    yield row.split(',')

        def check_total(total_cost: str) -> str:
            if total_cost > 100:
                return "お買い物検定3級"
            else:
                return "お買い物検定2級"

        def create_text(extract_date: str, result_dict: dict) -> str:
            text = f"{extract_date}のGCP使用料金"
            total_cost = 0
            for project, values in result_dict.items():
                text = text+f"\n{project}----"
                for value in sorted(
                        values, key=itemgetter('cost'), reverse=True):
                    cost = int(float(value['cost']))
                    text = text+f"\n・{value['service']} ¥{cost}"
                    total_cost += cost
            text = text+f"\n\n{check_total(total_cost)}"
            return text

        def transform(**kwargs):
            filepath = kwargs['ti'].xcom_pull(key='local_log_filepath')
            result_dict = {}
            for row in reader(filepath):
                if result_dict.get(row[0], None) is not None:
                    result_dict[row[0]] = \
                        list(result_dict[row[0]])\
                        + [dict(service=row[1], cost=row[2])]
                else:
                    result_dict[row[0]] = \
                        [dict(service=row[1], cost=row[2])]
            kwargs['ti'].xcom_push(
                key='slack_text',
                value=create_text(kwargs['extract_date'], result_dict))

        extract_task = BigQueryOperator(
            task_id='extract',
            sql='sql/extract_cost.sql',
            filepath="{{ ti.xcom_pull(key='local_log_filepath') }}",
            do_xcom_push=False,
        )

        transform_task = PythonOperator(
            task_id="transform",
            op_kwargs={"extract_date": '{{ extract_date(ti) }}'},
            python_callable=transform,
        )

        slack_notify_task = SlackAPIPostOperatorHatiware(
            task_id="slack_notify",
            text="{{ ti.xcom_pull(key='slack_text')}}",
            channel="costs"
        )

        extract_task >> transform_task >> slack_notify_task

    with TaskGroup(
         "clean_group", tooltip="bqデータの整理") as clean_group:

        clean_bq_task = BigQueryOperator(
            task_id='clean_bq',
            sql="DELETE FROM {{ bq_table }} \
                 WHERE cast(export_time as date) <= '{{ delete_date(ti) }}'",
            do_xcom_push=False,
        )

        clean_bq_task

    prepare_task >> main_group >> clean_group
