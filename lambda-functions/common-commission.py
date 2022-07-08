import time
import boto3


DATABASE = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
output = 's3://xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx/'


def lambda_handler(event, context):
    query = '''INSERT INTO "devflows_partner_feeds_pipeline"."common_commission_table"
                SELECT
                    O.supplierid AS Supplier,
                    split(conversions.conversion_data.publisher_reference, '.')[1] AS TenantID,
                    Con.tenantname AS TenantName,
                    split(conversions.conversion_data.publisher_reference, '.')[2] AS CampaignID,
                    Cam.campaignname AS CampaignName,
                    O.id AS OfferID,
                    O.name AS OfferName,
                    conversions.conversion_data.country AS Country,
                    conversions.conversion_data.currency AS Currency,
                    A.subscriberid AS SubscriberID,
                    conversions.conversion_data.conversion_time AS SubscriptionDate,
                    conversion_items.item_value,
                    conversions.conversion_data.conversion_value.value AS conversion_value,
                    conversions.conversion_data.conversion_value.publisher_commission AS PublisherCommission,
                    conversions.conversion_data.conversion_id AS ConversionID,
                    split(conversions.conversion_data.publisher_reference, '.')[3] AS LinkID,
                    P."context"."upload-time" AS LoadTime
                FROM "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"."apple_devflows_json_outputs" AS P
                CROSS JOIN UNNEST(P.data.http_response_object.conversions) AS t(conversions)
                CROSS JOIN UNNEST(conversions.conversion_data.conversion_items) AS t(conversion_items)
                JOIN "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"."activationlink_refreshed" AS A
                ON split(conversions.conversion_data.publisher_reference, '.')[3] = A.id
                JOIN "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"."offer_refreshed" AS O
                ON A.offerId = O.id
                JOIN "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"."config_refreshed" AS Con
                ON Con.id = split(conversions.conversion_data.publisher_reference, '.')[1]
                JOIN "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"."campaign_refreshed" AS Cam
                ON Cam.id = split(conversions.conversion_data.publisher_reference, '.')[2]
                WHERE conversions.conversion_data.publisher_reference LIKE '%.%'
                AND CAST(REPLACE(P."context"."upload-time", 'T', ' ') AS timestamp) > date_add('day', -24, now());'''
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
