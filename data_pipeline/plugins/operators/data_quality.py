from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 tables=None,
                 validations=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        
        self.conn_id = conn_id
        self.tables = tables
        self.validations = validations


    def execute(self, context):
        redshift_hook = PostgresHook(self.conn_id)
        for table in self.tables:
            for i, validation in enumerate(self.validations[table]):
                records = redshift_hook.get_records(validation["test_sql"])
                if not validation["expected_results"] == records[0][0]:
                    raise ValueError(f"Data quality check #{i} failed. the table {table} requires to be reviewed")
                else:
                    self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
