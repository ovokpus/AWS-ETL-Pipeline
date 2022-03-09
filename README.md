# AWS-ETL-Pipeline

---

<div>
	<img src="https://github.com/ovokpus/AWS-ETL-Pipeline/blob/main/images/architecture.jpg">
</div>

Data Engineering project with AWS Resources, orchestrated with Devflows

---

## Tools and Technologies utilized

### Devflows

An ETL tool that allows us to build pipelines and applications by stitching together AWS services and SaaS applications in a visual interface.

### AWS Resources and Services

- DynamoDB
- AWS Athena
- AWS Glue Catalogs
- AWS Glue ETL pipelines
- S3
- AWS Lambda
- Amazon EventBridge
- Amazon SQS (Simple Queue Service)
- IAM

### Data Sources

1. The first phase of the project involves pulling JSON feeds from the Apple Partnerize API. The endpoint in this use case is the Partner Conversions endpoint:

https://appleservices-api.partnerize.com/reporting/report_publisher/publisher/{publisher_id}/conversion.json?

2. The other Data Source is from Four Existing DynamoDB tables

The final Data Structure is an output of joining these tables with the JSON Data Feeds from the Apple Partnerize API

---

### The common data structure

This is table data that can be queried from Amazon Athena (with both SELECT and INSERT statements). A SELECT query against this table produces a csv file which can be viewed from an S3 location

---

### The Daily Payment advice Query

This is a query off the Common Commission table that retrieves data from the JSON feeds that has been enriched from DynamoDB.

---

### The Daily Telco Notification

This is another query from the Common Commission Table with selected columns specifically for notifying various Telcos on a daily basis

---

### Data Staging Areas

At various stages of the pipeline, data will be staged within various S3 locations, based on the workflow within the AWS Glue Pipeline:

1. From the Devflows step, JSON files are staged in an S3 location

2. From the AWS Glue step, DynamoDB tables are staged in another S3 location.

3. The S3 location for the common commission table is staged here: s3://totogi-marketplace-partner-feeds/common-commission-table/. This location stores the table data in parquet format, which is one of the formats that accepts insert statements from Athena.

---
