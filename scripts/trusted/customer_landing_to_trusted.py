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

# Script generated for node S3 - Customer Landing
S3CustomerLanding_node1701033045996 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://wzlng-stedi/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3CustomerLanding_node1701033045996",
)

# Script generated for node SQL Filter
SqlQuery0 = """
select * from customer_landing
where shareWithResearchAsOfDate is not null;
"""
SQLFilter_node1701033118681 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"customer_landing": S3CustomerLanding_node1701033045996},
    transformation_ctx="SQLFilter_node1701033118681",
)

# Script generated for node S3 - Customer Trusted
S3CustomerTrusted_node1701033754978 = glueContext.getSink(
    path="s3://wzlng-stedi/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3CustomerTrusted_node1701033754978",
)
S3CustomerTrusted_node1701033754978.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
S3CustomerTrusted_node1701033754978.setFormat("json")
S3CustomerTrusted_node1701033754978.writeFrame(SQLFilter_node1701033118681)
job.commit()
