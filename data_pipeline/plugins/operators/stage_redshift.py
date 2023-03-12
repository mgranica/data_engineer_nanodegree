from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        json '{}'
        REGION 'us-west-2'
    """

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 aws_credentials="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 s3_json_path="auto",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_json_path = s3_json_path
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        
        self.log.info('Clearing data from destination Redshift table')
        redshift.run("DELETE FROM {}".format(self.table))
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        self.log.info("Copying data from S3 to Redshift")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.s3_json_path
        )
        redshift.run(str(formatted_sql))





