from google.cloud import bigquery
from google.oauth2 import service_account
from airflow.models import Variable
from airflow.models import BaseOperator

class BigQueryOperator(BaseOperator):

    template_fields = ('sql','filepath')
    template_ext = ('.sql')
    ui_color = '#db7093'

    def __init__(self, sql, filepath=None, *args, **kwargs):
        super(BigQueryOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.filepath = filepath


    def execute(self, *args, **kwargs):
        _credentials = service_account.Credentials.from_service_account_file(
            Variable.get("bigquery_sa_keypath"),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        _client = bigquery.Client(
            credentials=_credentials,
            project=_credentials.project_id,
        )
        query_job = _client.query(self.sql)
        result = query_job.result()

        if self.filepath is not None:
            with open(self.filepath, 'w') as f:
                for rows in result:
                    [f.write(f"{row},") for row in rows]
                    f.write("\n")
        else:
            return
