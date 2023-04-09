import os
import json
import logging
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secretsmanager = boto3.client('secretsmanager')
rdsdata = boto3.client('rds-data')

def get_secret(secret_arn):
    response = secretsmanager.get_secret_value(SecretId=secret_arn)
    secret = json.loads(response['SecretString'])
    return secret

def execute_statement(secret_arn, statement, resource_arn, database):
    response = rdsdata.execute_statement(
        secretArn=secret_arn,
        resourceArn=resource_arn,
        sql=statement,
        database=database
    )
    return response

def handler(event, context):
    secret_arn = os.environ['DB_SECRET_ARN_GlueCatalogDemo']
    secret = get_secret(secret_arn)
    resource_arn = os.environ['DB_CLUSTER_ARN']
    #statement = 'SELECT * FROM user;'
    q_create = 'CREATE TABLE IF NOT EXISTS user (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255));'
    q_insert = "INSERT INTO user (name) VALUES ('John Doe')"
    response = execute_statement(secret_arn, q_create, resource_arn, secret["dbname"])
    response = execute_statement(secret_arn, q_insert, resource_arn, secret["dbname"])
    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }
