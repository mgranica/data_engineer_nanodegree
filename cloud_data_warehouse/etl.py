import configparser
import psycopg2
import os
from sql_queries import copy_table_queries, insert_table_queries
import boto3
import json

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    filepath = os.path.join('dwh_cluster','dwh.cfg')
    config.read(filepath)
    
    KEY                   = config.get('AWS','KEY')
    SECRET                = config.get('AWS','SECRET')
    # Get Cluster Params
    DB_NAME               = config.get("CLUSTER","DB_NAME")
    DB_USER               = config.get("CLUSTER","DB_USER")
    DB_PASSWORD           = config.get("CLUSTER","DB_PASSWORD")
    DB_PORT               = config.get("CLUSTER","DB_PORT")
    DB_CLUSTER_IDENTIFIER = config.get("CLUSTER","DB_CLUSTER_IDENTIFIER")
    REGION_NAME           = config.get("CLUSTER", "REGION_NAME")
    # Get ENDPOINT 
    redshift = boto3.client('redshift',
                           region_name=REGION_NAME,
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                           )
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DB_CLUSTER_IDENTIFIER)['Clusters'][0]
    DB_ENDPOINT = myClusterProps['Endpoint']['Address']

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DB_ENDPOINT, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()