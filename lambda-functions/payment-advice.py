import time
from datetime import date
import boto3

today = date.today()

DATABASE = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
output = 's3://xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx/{}/{:02d}/daily-advice-for-{}/'.format(
    today.year, today.month, today)


def lambda_handler(event, context):
    query = '''SELECT * FROM "dxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"."xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                WHERE CAST(REPLACE(loadtime, 'T', ' ') AS timestamp) > date_add('hour', -24, now());'''
    client = boto3.client('athena')
    # Execution
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': output,
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
            }
        }
    )
    return response
