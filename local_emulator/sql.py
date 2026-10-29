"""DuckDB SQL that stands in for the Athena queries.

The production statements live in ``athena-queries/queries.sql`` and in the
three Lambda handlers. Presto's ``split(publisher_reference, '.')[1]`` is
1-based. DuckDB's ``split_part(..., 1)`` is 1-based as well, so the tenant,
campaign, and link positions stay the same.

``upload-time`` is forced to VARCHAR. DuckDB would otherwise infer a
timestamp and skip the ``REPLACE(..., 'T', ' ')`` cast the Lambdas rely on.
A blank upload time is turned into NULL inside a ``CASE`` before that cast.
Athena would attempt the cast on every row; a blank value makes that cast
fragile, and the UNLOAD query already rejects blanks with ``!= ''``.
"""

from __future__ import annotations

# Explicit schema so hyphenated JSON keys and the word "value" survive the read.
FEED_COLUMNS = """{
  context: 'STRUCT("upload-time" VARCHAR, "last-successful-load" VARCHAR)',
  data: 'STRUCT(http_response_object STRUCT(
    start_date_time_utc VARCHAR,
    end_date_time_utc VARCHAR,
    conversions STRUCT(conversion_data STRUCT(
      publisher_reference VARCHAR,
      country VARCHAR,
      currency VARCHAR,
      conversion_time VARCHAR,
      conversion_id VARCHAR,
      conversion_items STRUCT(
        item_value DOUBLE,
        category VARCHAR,
        conversion_item_id VARCHAR
      )[],
      conversion_value STRUCT("value" DOUBLE, publisher_commission DOUBLE)
    ))[]
  ))'
}"""


def common_commission_select(
    feed_glob: str,
    config_path: str,
    campaign_path: str,
    offer_path: str,
    activation_path: str,
) -> str:
    """SELECT that builds the common commission grain: one row per item.

    Join order matches ``lambda-functions/common-commission.py``:
    activation link, then offer, tenant config, and campaign. Unmatched
    publisher references disappear here because every join is an inner join.
    The 24-day predicate matches ``date_add('day', -24, now())`` against the
    bound ``pipeline_clock.commission_cutoff``.
    """
    return f"""
SELECT
    offer.supplierid AS supplier,
    split_part(conversions.conversion_data.publisher_reference, '.', 1) AS tenant_id,
    tenant_config.tenantname AS tenant_name,
    split_part(conversions.conversion_data.publisher_reference, '.', 2) AS campaign_id,
    campaign.campaignname AS campaign_name,
    offer.id AS offer_id,
    offer.name AS offer_name,
    conversions.conversion_data.country AS country,
    conversions.conversion_data.currency AS currency,
    activation.subscriberid AS subscriber_id,
    conversions.conversion_data.conversion_time AS subscription_date,
    conversion_items.item_value AS item_value,
    conversions.conversion_data.conversion_value."value" AS conversion_value,
    conversions.conversion_data.conversion_value.publisher_commission AS publisher_commission,
    conversions.conversion_data.conversion_id AS conversion_id,
    split_part(conversions.conversion_data.publisher_reference, '.', 3) AS link_id,
    p.context."upload-time" AS load_time
FROM read_json('{feed_glob}', columns={FEED_COLUMNS}, union_by_name=true) AS p,
UNNEST(p.data.http_response_object.conversions) AS u(conversions),
UNNEST(conversions.conversion_data.conversion_items) AS i(conversion_items)
JOIN read_json_auto('{activation_path}') AS activation
    ON split_part(conversions.conversion_data.publisher_reference, '.', 3) = activation.id
JOIN read_json_auto('{offer_path}') AS offer
    ON activation."offerId" = offer.id
JOIN read_json_auto('{config_path}') AS tenant_config
    ON tenant_config.id = split_part(conversions.conversion_data.publisher_reference, '.', 1)
JOIN read_json_auto('{campaign_path}') AS campaign
    ON campaign.id = split_part(conversions.conversion_data.publisher_reference, '.', 2)
WHERE conversions.conversion_data.publisher_reference LIKE '%.%'
  AND p.context."upload-time" != ''
  AND CASE
        WHEN p.context."upload-time" = '' THEN NULL
        ELSE CAST(replace(p.context."upload-time", 'T', ' ') AS TIMESTAMP)
      END > (SELECT commission_cutoff FROM pipeline_clock)
"""


# Payment advice is ``SELECT *`` plus the 24-hour predicate from payment-advice.py.
PAYMENT_ADVICE_SELECT = """
SELECT *
FROM common_commission
WHERE CAST(replace(load_time, 'T', ' ') AS TIMESTAMP)
    > (SELECT hour_cutoff FROM pipeline_clock)
"""

# Column list matches telco-notification.py. The tenant predicate is bound
# with a parameter so a tenant name cannot change the statement.
NOTIFICATION_SELECT = """
SELECT
    supplier,
    tenant_name,
    campaign_name,
    offer_name,
    country,
    currency,
    subscriber_id,
    subscription_date
FROM common_commission
WHERE tenant_name = ?
  AND CAST(replace(load_time, 'T', ' ') AS TIMESTAMP)
      > (SELECT hour_cutoff FROM pipeline_clock)
"""

TENANT_LIST_SELECT = """
SELECT DISTINCT tenant_name
FROM common_commission
ORDER BY tenant_name
"""
