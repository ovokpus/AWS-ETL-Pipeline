import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node DynamoDB table
DynamoDBtable_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    table_name="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    transformation_ctx="DynamoDBtable_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = DynamicFrame.fromDF(
    DynamoDBtable_node1.toDF().dropDuplicates(), glueContext, "ApplyMapping_node2"
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    catalogTableName="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(ApplyMapping_node2)
job.commit()
