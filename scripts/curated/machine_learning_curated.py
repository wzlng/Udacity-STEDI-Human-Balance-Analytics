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

# Script generated for node S3 - Accelerometer Trusted
S3AccelerometerTrusted_node1701064438297 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://wzlng-stedi/accelerometer/trusted/"],
            "recurse": True,
        },
        transformation_ctx="S3AccelerometerTrusted_node1701064438297",
    )
)

# Script generated for node S3 - Customer Curated
S3CustomerCurated_node1701064352888 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://wzlng-stedi/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="S3CustomerCurated_node1701064352888",
)

# Script generated for node S3 - Step Trainer Trusted
S3StepTrainerTrusted_node1701064490917 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://wzlng-stedi/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="S3StepTrainerTrusted_node1701064490917",
)

# Script generated for node SQL Join
SqlQuery762 = """
select * from step_trainer_trusted stt
join accelerometer_trusted at on stt.sensorReadingTime = at.timeStamp
join customer_curated cc on stt.serialNumber = cc.serialNumber;

"""
SQLJoin_node1701064543623 = sparkSqlQuery(
    glueContext,
    query=SqlQuery762,
    mapping={
        "step_trainer_trusted": S3StepTrainerTrusted_node1701064490917,
        "accelerometer_trusted": S3AccelerometerTrusted_node1701064438297,
        "customer_curated": S3CustomerCurated_node1701064352888,
    },
    transformation_ctx="SQLJoin_node1701064543623",
)

# Script generated for node S3 - Machine Learning Curated
S3MachineLearningCurated_node1701064708705 = glueContext.getSink(
    path="s3://wzlng-stedi/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3MachineLearningCurated_node1701064708705",
)
S3MachineLearningCurated_node1701064708705.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
S3MachineLearningCurated_node1701064708705.setFormat("json")
S3MachineLearningCurated_node1701064708705.writeFrame(SQLJoin_node1701064543623)
job.commit()
