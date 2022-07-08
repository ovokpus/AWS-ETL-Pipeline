import time
from datetime import date
import boto3

today = date.today()
client = boto3.client('athena')

DATABASE = 'devflows_partner_feeds_pipeline'
select_output = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx/'


def create_tenant_list():
    query = '''SELECT DISTINCT tenantname 
            FROM "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" '''

    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': select_output,
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
            }
        }
    )
    execution_id = response['QueryExecutionId']
    time.sleep(2)
    results = client.get_query_results(
        QueryExecutionId=execution_id,
    )
    result_set = results['ResultSet']['Rows'][1:]
    result_list = []
    for d in result_set:
        value = d['Data'][0]['VarCharValue']
        print(value)
        result_list.append(value)
    return result_list


def lambda_handler(event, context):
    result_list = create_tenant_list()
    response_dict = {}
    for result in result_list:
        output = 's3://xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx-{}/{}/{:02d}/daily-notification-for-{}/'.format(
            result.lower(), today.year, today.month, today)
        query = f'''SELECT
                     supplier,
                     tenantname,
                     campaignname,
                     offername,
                     country,
                     currency,
                     subscriberid,
                     subscriptiondate
                FROM "dxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"."xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                WHERE tenantname = '{result}'
                AND CAST(REPLACE(loadtime, 'T', ' ') AS timestamp) > date_add('hour', -24, now());'''

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
        response_dict[result] = response
    return response_dict
