-- QUERY THAT CREATED PARQUET FILE IN S3, WHICH WAS CRAWLED WITH GLUE

UNLOAD(
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
FROM "devflows_partner_feeds_pipeline"."devflows_json_outputs" AS P
CROSS JOIN UNNEST(P.data.http_response_object.conversions) AS t(conversions)
CROSS JOIN UNNEST(conversions.conversion_data.conversion_items) AS t(conversion_items)
JOIN "devflows_partner_feeds_pipeline"."activationlink_refreshed" AS A
ON split(conversions.conversion_data.publisher_reference, '.')[3] = A.id
JOIN "devflows_partner_feeds_pipeline"."offer_refreshed" AS O
ON A.offerId = O.id
JOIN "devflows_partner_feeds_pipeline"."config_refreshed" AS Con
ON Con.id = split(conversions.conversion_data.publisher_reference, '.')[1]
JOIN "devflows_partner_feeds_pipeline"."campaign_refreshed" AS Cam
ON Cam.id = split(conversions.conversion_data.publisher_reference, '.')[2]
WHERE conversions.conversion_data.publisher_reference LIKE '%.%'
AND P."context"."upload-time" != '')
TO 's3://totogi-marketplace-partner-feeds/common-commission-table/'
WITH (format = 'PARQUET', compression = 'SNAPPY')



--QUERYING JSON FILE IN S3, WHICH WAS CRAWLED WITH GLUE
SELECT
    conversions.conversion_data.conversion_id,
    split(conversions.conversion_data.publisher_reference, '.')[1] AS tenant_id,
    split(conversions.conversion_data.publisher_reference, '.')[2] AS campaign_id,
    split(conversions.conversion_data.publisher_reference, '.')[3] AS link_id,
    conversions.conversion_data.conversion_time AS conversion_date,
    conversions.conversion_data.currency,
    conversions.conversion_data.country,
    conversion_items.category,
    conversion_items.conversion_item_id,
    conversion_items.item_value,
    conversions.conversion_data.conversion_value.value AS conversion_value,
    conversions.conversion_data.conversion_value.publisher_commission,
    data.http_response_object.start_date_time_utc AS ReportStartTime,
    data.http_response_object.end_date_time_utc AS ReportEndTime,
    "context"."upload-time" AS LoadTime,
    "context"."last-successful-load" AS LastSuccessfulLoad
FROM "devflows_partner_feeds_pipeline"."apple_devflows_json_outputs"
CROSS JOIN UNNEST(data.http_response_object.conversions) AS t(conversions)
CROSS JOIN UNNEST(conversions.conversion_data.conversion_items) AS t(conversion_items)
WHERE conversions.conversion_data.publisher_reference LIKE '%.%';