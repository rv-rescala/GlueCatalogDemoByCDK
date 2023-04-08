import os
import json
import logging
import pymysql
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secretsmanager = boto3.client('secretsmanager')

def get_secret(secret_arn):
    response = secretsmanager.get_secret_value(SecretId=secret_arn)
    secret = json.loads(response['SecretString'])
    return secret

def create_connection(secret):
    return pymysql.connect(
        host=secret['host'],
        port=int(secret['port']),
        user=secret['username'],
        password=secret['password'],
        database=secret['dbname'],
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def create_user_table(connection):
    with connection.cursor() as cursor:
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS user (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(255) NOT NULL UNIQUE
        )
        """
        cursor.execute(create_table_sql)
        connection.commit()

def handler(event, context):
    secret_arn = os.environ['DB_SECRET_ARN_GlueCatalogDemo']
    secret = get_secret(secret_arn)
    connection = create_connection(secret)
    create_user_table(connection)
    connection.close()