import enum

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Sequence, Set, SupportsAbs, Union


class BigQueryUIColors(enum.Enum):
    """Hex colors for BigQuery operators"""

    CHECK = "#C0D7FF"
    QUERY = "#A1BBFF"
    TABLE = "#81A0FF"
    DATASET = "#5F86FF"

class BigQueryGetDataOperator(BaseOperator):
    """
    Fetches the data from a BigQuery table (alternatively fetch data for selected columns)
    and returns data in a python list. The number of elements in the returned list will
    be equal to the number of rows fetched. Each element in the list will again be a list
    where element would represent the columns values for that row.
    **Example Result**: ``[['Tony', '10'], ['Mike', '20'], ['Steve', '15']]``
    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryGetDataOperator`
    .. note::
        If you pass fields to ``selected_fields`` which are in different order than the
        order of columns already in
        BQ table, the data will still be in the order of BQ table.
        For example if the BQ table has 3 columns as
        ``[A,B,C]`` and you pass 'B,A' in the ``selected_fields``
        the data would still be of the form ``'A,B'``.
    **Example**: ::
        get_data = BigQueryGetDataOperator(
            task_id='get_data_from_bq',
            dataset_id='test_dataset',
            table_id='Transaction_partitions',
            max_results=100,
            selected_fields='DATE',
            gcp_conn_id='airflow-conn-id'
        )
    :param dataset_id: The dataset ID of the requested table. (templated)
    :param table_id: The table ID of the requested table. (templated)
    :param max_results: The maximum number of records (rows) to be fetched
        from the table. (templated)
    :param selected_fields: List of fields to return (comma-separated). If
        unspecified, all fields are returned.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param location: The location used for the operation.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        'dataset_id',
        'table_id',
        'max_results',
        'selected_fields',
        'impersonation_chain',
    )
    ui_color = BigQueryUIColors.QUERY.value

    def __init__(
        self,
        *,
        dataset_id: str,
        table_id: str,
        max_results: int = 100,
        selected_fields: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        bigquery_conn_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        location: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.",
                DeprecationWarning,
                stacklevel=3,
            )
            gcp_conn_id = bigquery_conn_id

        self.dataset_id = dataset_id
        self.table_id = table_id
        self.max_results = int(max_results)
        self.selected_fields = selected_fields
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.location = location
        self.impersonation_chain = impersonation_chain

    def execute(self, context: 'Context') -> list:
        self.log.info(
            'Fetching Data from %s.%s max results: %s', self.dataset_id, self.table_id, self.max_results
        )

        hook = BigQueryHook(
            bigquery_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        rows = hook.list_rows(
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            max_results=self.max_results,
            selected_fields=self.selected_fields,
            location=self.location,
        )

        self.log.info('Total extracted rows: %s', len(rows))

        table_data = [row.values() for row in rows]
        return table_data