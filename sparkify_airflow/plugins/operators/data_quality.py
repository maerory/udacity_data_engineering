from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    check_total_query = """
        SELECT COUNT(*)
        FROM {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        for table in self.tables:
            record_count = redshift_hook.get_records(check_query.format(table))
            if len(record_count) < 1 or len(record_count[0]) < 1 or record[0][0] < 1:
                self.log.error(f"Error in {table}: Data Quality Check Failed")
                raise ValueError(f"Error in {table}: Data Quality Check Failed")
            else:
                self.log.info(f"{Table} Data Quality Check Complete")
