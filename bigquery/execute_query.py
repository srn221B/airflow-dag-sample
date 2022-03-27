from google.cloud import bigquery
from google.oauth2 import service_account
from airflow.models import Variable


class ExecuteQuery:

    def __init__(self):
        _credentials = service_account.Credentials.from_service_account_file(
            Variable.get("bigquery_sa_keypath"),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.client = bigquery.Client(
            credentials=_credentials,
            project=_credentials.project_id,
        )

    def execute(self, query: str) -> list:
        query_job = self.client(query)
        return query_job.result()
