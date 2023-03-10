from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 sql_query="",
                 table="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.sql_query = sql_query
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id = self.conn_id)
        
        if self.truncate:
            self.log.info(f"Clearing data from dimension table {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")
            
        self.log.info(f"Loading dimension table {self.table}")        
        redshift.run(self.sql_query)
        
