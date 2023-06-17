import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1687037629803 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://data-engineer-youtube-cleaned-useast1-dev/cleaned-data/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1687037629803",
)

# Script generated for node Amazon S3
AmazonS3_node1687037697917 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": [
            "s3://data-engineer-youtube-cleaned-useast1-dev/youtube/raw_statistics/"
        ],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1687037697917",
)

# Script generated for node Join
Join_node1687037723549 = Join.apply(
    frame1=AmazonS3_node1687037629803,
    frame2=AmazonS3_node1687037697917,
    keys1=["id"],
    keys2=["category_id"],
    transformation_ctx="Join_node1687037723549",
)

# Script generated for node Amazon S3
AmazonS3_node1687037850219 = glueContext.getSink(
    path="s3://data-engineer-youtube-analytics-useast1-dev",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1687037850219",
)
AmazonS3_node1687037850219.setCatalogInfo(
    catalogDatabase="db_youtube_analytics", catalogTableName="final-analytics"
)
AmazonS3_node1687037850219.setFormat("glueparquet")
AmazonS3_node1687037850219.writeFrame(Join_node1687037723549)
job.commit()
