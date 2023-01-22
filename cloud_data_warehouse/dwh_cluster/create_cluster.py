import pandas as pd
import boto3
import json
import time
import configparser
import os
from botocore.exceptions import ClientError



def load_dwh_params():
    config = configparser.ConfigParser()
    filepath = os.path.join('dwh_cluster','dwh.cfg')
    config.read(filepath)
    # AWS
    KEY                   = config.get('AWS','KEY')
    SECRET                = config.get('AWS','SECRET')
    # Cluster
    DB_CLUSTER_TYPE       = config.get("CLUSTER","DB_CLUSTER_TYPE")
    DB_NUM_NODES          = config.get("CLUSTER","DB_NUM_NODES")
    DB_NODE_TYPE          = config.get("CLUSTER","DB_NODE_TYPE")
    DB_CLUSTER_IDENTIFIER = config.get("CLUSTER","DB_CLUSTER_IDENTIFIER")
    DB_NAME               = config.get("CLUSTER","DB_NAME")
    DB_USER               = config.get("CLUSTER","DB_USER")
    DB_PASSWORD           = config.get("CLUSTER","DB_PASSWORD")
    DB_PORT               = config.get("CLUSTER","DB_PORT")
    DB_IAM_ROLE_NAME      = config.get("CLUSTER", "DB_IAM_ROLE_NAME")
    REGION_NAME           = config.get("CLUSTER", "REGION_NAME")
    
    return KEY, SECRET, DB_CLUSTER_TYPE, DB_NUM_NODES, DB_NODE_TYPE, DB_CLUSTER_IDENTIFIER, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, DB_IAM_ROLE_NAME, REGION_NAME


def get_aws_resources(KEY: str, SECRET: str, REGION_NAME: str):
    ec2 = boto3.resource('ec2',
                           region_name=REGION_NAME,
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                        )

    s3 = boto3.resource('s3',
                           region_name=REGION_NAME,
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                           )

    iam = boto3.client('iam',
                           region_name=REGION_NAME,
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET,
                          )

    redshift = boto3.client('redshift',
                           region_name=REGION_NAME,
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                           )
    return ec2, s3, iam, redshift

def create_iam_role(iam, DB_IAM_ROLE_NAME: str):
    
    # Create the role
    try:
        print('Creating a new IAM Role')
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DB_IAM_ROLE_NAME,
            Description = 'Allows Redshift clusters to call AWS services on your behalf.',
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)
    print("Attaching Policy")
    iam.attach_role_policy(RoleName=DB_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']
    print("Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DB_IAM_ROLE_NAME)['Role']['Arn']
    print("IamRole ARN extraction: done")
    return roleArn

def create_redshift_cluster(
    redshift,
    DB_CLUSTER_TYPE,
    DB_NODE_TYPE,
    DB_NUM_NODES,
    DB_CLUSTER_IDENTIFIER,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    roleArn
):
    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=DB_CLUSTER_TYPE,
            NodeType=DB_NODE_TYPE,
            NumberOfNodes=int(DB_NUM_NODES),

            #Identifiers & Credentials
            DBName=DB_NAME,
            ClusterIdentifier=DB_CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,

            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
        print("Redshift Cluster creaded correctly.")
    except Exception as e:
        print(e)
    condition= True
    while condition:
        time.sleep(10)
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DB_CLUSTER_IDENTIFIER)['Clusters'][0]
        cluster_status = myClusterProps['ClusterStatus']
        print(f"Cluster status: {cluster_status}")
        if cluster_status == "available":
            DB_ENDPOINT = myClusterProps['Endpoint']['Address']
            DB_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
            return myClusterProps, DB_ENDPOINT, DB_ROLE_ARN
            

def open_TCP(ec2, myClusterProps, DB_PORT):
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT)
        )
    except Exception as e:
        print(e)
        
        
def cluster_connection(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB_NAME):
    print("check cluster connection")
    conn_string="postgresql://{}:{}@{}:{}/{}".format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB_NAME)
    print(conn_string)
    return conn_string
        


def main():
    
    KEY, SECRET, DB_CLUSTER_TYPE, DB_NUM_NODES, DB_NODE_TYPE, DB_CLUSTER_IDENTIFIER, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT, DB_IAM_ROLE_NAME, REGION_NAME = load_dwh_params()
    
    ec2, s3, iam, redshift = get_aws_resources(KEY, SECRET, REGION_NAME)
    
    roleArn = create_iam_role(iam, DB_IAM_ROLE_NAME)
    
    myClusterProps, DB_ENDPOINT, DB_ROLE_ARN = create_redshift_cluster(redshift, DB_CLUSTER_TYPE, DB_NODE_TYPE, DB_NUM_NODES, DB_CLUSTER_IDENTIFIER, DB_NAME, DB_USER, DB_PASSWORD, roleArn)
    open_TCP(ec2, myClusterProps, DB_PORT)
    
    conn = cluster_connection(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB_NAME)
    
  
if __name__ == "__main__":
    main()

