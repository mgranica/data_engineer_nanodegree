import pandas as pd
import boto3
import json
import configparser
import os


def delete_cluster(redshift, DB_CLUSTER_IDENTIFIER):
    redshift.delete_cluster( ClusterIdentifier=DB_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    
def delete_iam_role(iam, DB_IAM_ROLE_NAME):
    iam.detach_role_policy(RoleName=DB_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DB_IAM_ROLE_NAME)
    

def main():
    config = configparser.ConfigParser()
    filepath = os.path.join('dwh_cluster','dwh.cfg')
    config.read(filepath)
    
    # AWS
    KEY                   = config.get('AWS','KEY')
    SECRET                = config.get('AWS','SECRET')
    # CLUSTER
    DB_CLUSTER_IDENTIFIER = config.get("CLUSTER", "DB_CLUSTER_IDENTIFIER")
    DB_IAM_ROLE_NAME      = config.get("CLUSTER", "DB_IAM_ROLE_NAME")
    REGION_NAME           = config.get("CLUSTER", "REGION_NAME")
    
    iam = boto3.client('iam',aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         region_name=REGION_NAME,
                      )

    redshift = boto3.client('redshift',
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET,
                           region_name=REGION_NAME,
                           )
    
    delete_cluster(redshift, DB_CLUSTER_IDENTIFIER)
    
    delete_iam_role(iam, DB_IAM_ROLE_NAME)

if __name__ == "__main__":
    main()