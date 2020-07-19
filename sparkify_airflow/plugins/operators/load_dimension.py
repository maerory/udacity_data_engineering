from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 sql_query= "",
                 delete=False,
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.delete= delete

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.delete_load:
            redshift_hook.run(f"DELETE FROM {self.table_name}")
            self.log.info(f"{self.table} Dimension Table Deleted Before Loading")

        redshift_hook.run(self.sql_query)
        self.log.info(f"{self.table} Dimension Table Loading Complete.")
