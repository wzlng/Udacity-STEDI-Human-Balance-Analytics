import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


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

# Script generated for node S3 - Customer Trusted
S3CustomerTrusted_node1701036995561 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://wzlng-stedi/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3CustomerTrusted_node1701036995561",
)

# Script generated for node S3 - Accelerometer Landing
S3AccelerometerLanding_node1701035992703 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://wzlng-stedi/accelerometer/landing/"],
            "recurse": True,
        },
        transformation_ctx="S3AccelerometerLanding_node1701035992703",
    )
)

# Script generated for node SQL Join
SqlQuery804 = """
select al.* from accelerometer_landing al
join customer_trusted ct on al.user = ct.email;
"""
SQLJoin_node1701035996350 = sparkSqlQuery(
    glueContext,
    query=SqlQuery804,
    mapping={
        "accelerometer_landing": S3AccelerometerLanding_node1701035992703,
        "customer_trusted": S3CustomerTrusted_node1701036995561,
    },
    transformation_ctx="SQLJoin_node1701035996350",
)

# Script generated for node S3 - Accelerometer Trusted
S3AccelerometerTrusted_node1701036005060 = glueContext.getSink(
    path="s3://wzlng-stedi/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3AccelerometerTrusted_node1701036005060",
)
S3AccelerometerTrusted_node1701036005060.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
S3AccelerometerTrusted_node1701036005060.setFormat("json")
S3AccelerometerTrusted_node1701036005060.writeFrame(SQLJoin_node1701035996350)
job.commit()
