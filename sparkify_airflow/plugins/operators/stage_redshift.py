from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        TRUNCATECOLUMNS BLANKASNULL EMPTYASNULL
        FORMAT AS {} 'auto';
    """

    copy_backfill = """
        COPY {}
        FROM '{}/{}/{}/'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        TRUNCATECOLUMNS BLANKASNULL EMPTYASNULL
        FORMAT AS {} 'auto';
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 format="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.filename = table + "_data"
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.format = format
        self.aws_credentials_id = aws_credentials_id
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        redshift.run("DELETE FROM {}".format(self.table))

        s3_path = "s3://{}/{}".format(self.s3_bucket, self.filename)

        if self.execution_date is None:
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.format
            )
        else:
            formatted_sql =
            StageToRedshiftOperator.copy_backfill.format(
                self.table,
                s3_path,
                self.execution_date.strftime("%Y"),
                self.execution_date.strftime("%d"),
                credentials.access_key,
                credentials.secret_key,
                self.format
            )
        redshift.run(formmated_sql)
        self.log.info(f"Table {self.table} stage complete.")
