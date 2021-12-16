import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="nycitytaxianalysis",
    table_name="lab1green",                  # <-- change the table name to the table you created in lab 1
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("vendorid", "long", "vendorid", "long"),
        ("lpep_pickup_datetime", "string", "pickup_datetime", "string"),
        ("lpep_dropoff_datetime", "string", "dropoff_datetime", "string"),
        ("passenger_count", "long", "passenger_count", "long"),
        ("trip_distance", "double", "trip_distance", "double"),
        ("ratecodeid", "long", "ratecodeid", "long"),
        ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"),
        ("pulocationid", "long", "pulocationid", "long"),
        ("dolocationid", "long", "dolocationid", "long"),
        ("payment_type", "long", "payment_type", "long"),
        ("fare_amount", "double", "fare_amount", "double"),
        ("extra", "double", "extra", "double"),
        ("mta_tax", "double", "mta_tax", "double"),
        ("tip_amount", "double", "tip_amount", "double"),
        ("tolls_amount", "double", "tolls_amount", "double"),
        ("improvement_surcharge", "double", "improvement_surcharge", "double"),
        ("total_amount", "double", "total_amount", "double"),
        ("congestion_surcharge", "double", "congestion_surcharge", "double"),
        ("partition_0", "string", "partition_0", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script to add column
##Custom Transformation
#convert to a Spark DataFrame...
customDF = ApplyMapping_node2.toDF()

#add a new column for "type"
customDF = customDF.withColumn("type", lit('green'))

# Convert back to a DynamicFrame for further processing.
customDynamicFrame = DynamicFrame.fromDF(customDF, glueContext, "customDF_df")
##----------------------------------

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=customDynamicFrame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://<your bucket name>/sagemaker/DEMO-xgboost-tripfare/transformed/data/",  # <-- update the bucket name to the correct bucket
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node3",
)

job.commit()
