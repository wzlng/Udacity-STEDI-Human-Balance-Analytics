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

# Script generated for node S3 - Step Trainer Landing
S3StepTrainerLanding_node1701041105504 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://wzlng-stedi/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3StepTrainerLanding_node1701041105504",
)

# Script generated for node S3 - Customer Curated
S3CustomerCurated_node1701042173727 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://wzlng-stedi/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="S3CustomerCurated_node1701042173727",
)

# Script generated for node SQL Join
SqlQuery810 = """
select stl.* from step_trainer_landing stl
join customers_curated cc on stl.serialNumber = cc.serialNumber;
"""
SQLJoin_node1701041200288 = sparkSqlQuery(
    glueContext,
    query=SqlQuery810,
    mapping={
        "step_trainer_landing": S3StepTrainerLanding_node1701041105504,
        "customers_curated": S3CustomerCurated_node1701042173727,
    },
    transformation_ctx="SQLJoin_node1701041200288",
)

# Script generated for node S3 - Step Trainer Trusted
S3StepTrainerTrusted_node1701041345535 = glueContext.getSink(
    path="s3://wzlng-stedi/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3StepTrainerTrusted_node1701041345535",
)
S3StepTrainerTrusted_node1701041345535.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
S3StepTrainerTrusted_node1701041345535.setFormat("json")
S3StepTrainerTrusted_node1701041345535.writeFrame(SQLJoin_node1701041200288)
job.commit()
