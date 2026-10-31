# Business case: AWS ETL Pipeline

**Daily commission and payment-advice files for a subscription affiliate program, built on serverless AWS.**

## Who buys this

The buyer is the finance-operations or partner-operations lead at a company that resells or promotes third-party subscriptions through partners, for example a telecom bundling streaming or app subscriptions, or a marketplace that pays partners per conversion. The users are the finance analysts who reconcile commissions, and the partner managers who send each partner their daily figures.

## The problem and what it costs

Conversion events arrive from an affiliate network as JSON. Deciding who earns what means joining each conversion to tenant, campaign, offer and activation-link records. Done by hand in spreadsheets, that reconciliation is slow, error-prone and hard to audit. Gartner research puts the average cost of poor data quality at **at least US$12.9 million a year per organisation**, based on a self-reported 2020 survey of 154 reference customers of data-quality vendors ([Gartner](https://www.gartner.com/en/data-analytics/topics/data-quality)). It is quoted here as context, not as a measured saving.

The infrastructure side is easier to price. Athena charges **US$5 per TB scanned**, and columnar Parquet plus compression cuts the scanned bytes sharply. AWS's own example takes a query from $15 on 3 TB of text to $1.25 on Parquet ([AWS Athena pricing](https://aws.amazon.com/athena/pricing/)). This pipeline writes Parquet.

## How the repo solves it

| Need | What in the repo does it |
|---|---|
| Pull the conversion feed on a schedule | A scheduler, SQS and an HTTP request land the affiliate-network JSON in S3, and the last successful load time is stored |
| Keep reference data current | Four Glue jobs (`glue-scripts/`) copy the DynamoDB tenant, campaign, offer and activation-link tables to S3, de-duplicated |
| Attribute each conversion | The `publisher_reference` (`tenantId.campaignId.linkId`) is joined to those tables. Unmatched rows are dropped. |
| One commission table | `lambda-functions/common-commission.py` runs a daily Athena `INSERT` on EventBridge (`athena-queries/queries.sql`) |
| Finance and partner outputs | `payment-advice.py` writes a daily CSV for the last 24 hours. `telco-notification.py` writes one CSV per tenant. |
| A way to look at the rows | A local stand-in for S3, Glue and Athena, plus a Streamlit desk (`dashboard/`), run with `docker compose up` |

## Use-case scenarios

All of these organisations are fictional.

1. **A mobile operator bundling app subscriptions.** Each morning, finance receives one payment-advice file for the previous 24 hours. Each partner telco receives its own notification file with campaign, offer, country, currency and subscriber details.
2. **A cashback marketplace.** It reuses the attribution join to credit each conversion to the right campaign and partner, with an audit trail in S3 for each daily run.
3. **A finance analyst checking a partner dispute.** The analyst opens the Streamlit desk, filters by tenant and date, and finds the exact rows behind a partner's statement.

## ROI: a worked model (illustrative)

These figures are **illustrative assumptions**, apart from the Athena price.

| Input | Value | Basis |
|---|---|---|
| Analyst hours a week spent reconciling commissions by hand | 10 | Assumption |
| Share removed by the automated daily run | 80% | Assumption |
| Analyst hourly cost | US$70 | BLS May 2024 median for data scientists, used as a proxy for a data analyst: $112,590 a year, about $54 an hour ([BLS](https://www.bls.gov/ooh/math/data-scientists.htm)). Multiplied by an assumed 1.3 for overhead. |
| Data scanned per day by the three queries | 10 GB | Assumption |
| Athena price | US$5 per TB | [AWS](https://aws.amazon.com/athena/pricing/) |

Labour saved: 10 × 52 × 80% = **416 hours, about US$29,000 a year**. Athena cost: 10 GB × 365 ≈ 3.65 TB × $5 ≈ **US$18 a year** (plus S3, Glue and Lambda, which are also small at this volume). Most of the case is labour and audit quality, not compute.

## Path to production

1. Fix the known edge cases documented in the README. The commission window is 24 *days*, not hours, and both Lambda windows use a strict `>` that excludes rows exactly on the cutoff.
2. Replace the redacted catalog names with real ones through configuration, not code.
3. Add data-quality checks (unmatched references, duplicate conversions) and alert on them, instead of silently dropping rows.
4. Deliver partner files through a governed channel, with an access policy for each tenant.

`PRODUCTIONIZING.md` lists the engineering work.

## Risks and limits

- Attribution rules are business rules. They need sign-off from finance and the partners, not just code review.
- Unmatched conversions are dropped today, so money owed to partners could be missed unless the gaps are reported.
- The sample data is fictional, so throughput and cost need testing at real volume.

## Sources

- AWS, Amazon Athena pricing: https://aws.amazon.com/athena/pricing/
- Gartner, Data Quality topic page ($12.9M figure): https://www.gartner.com/en/data-analytics/topics/data-quality
- U.S. Bureau of Labor Statistics, Data Scientists: https://www.bls.gov/ooh/math/data-scientists.htm
