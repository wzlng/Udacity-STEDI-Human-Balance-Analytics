import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 - Accelerometer Landing
S3AccelerometerLanding_node1701039509625 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://wzlng-stedi/accelerometer/landing/"],
            "recurse": True,
        },
        transformation_ctx="S3AccelerometerLanding_node1701039509625",
    )
)

# Script generated for node S3 - Customer Trusted
S3CustomerTrusted_node1701038938024 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://wzlng-stedi/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3CustomerTrusted_node1701038938024",
)

# Script generated for node SQL Join
SqlQuery885 = """
select ct.* from customer_trusted ct
join accelerometer_landing al on ct.email = al.user;
"""
SQLJoin_node1701039559007 = sparkSqlQuery(
    glueContext,
    query=SqlQuery885,
    mapping={
        "customer_trusted": S3CustomerTrusted_node1701038938024,
        "accelerometer_landing": S3AccelerometerLanding_node1701039509625,
    },
    transformation_ctx="SQLJoin_node1701039559007",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1701063734514 = DynamicFrame.fromDF(
    SQLJoin_node1701039559007.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1701063734514",
)

# Script generated for node S3 - Customer Curated
S3CustomerCurated_node1701039037848 = glueContext.getSink(
    path="s3://wzlng-stedi/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3CustomerCurated_node1701039037848",
)
S3CustomerCurated_node1701039037848.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customers_curated"
)
S3CustomerCurated_node1701039037848.setFormat("json")
S3CustomerCurated_node1701039037848.writeFrame(DropDuplicates_node1701063734514)
job.commit()
