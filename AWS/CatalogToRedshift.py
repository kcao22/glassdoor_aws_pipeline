import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

import datetime
import pyspark.sql.functions as F
import pytz
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


date_format='%m/%d/%Y'
date = datetime.datetime.now(tz=pytz.utc)
date = date.astimezone(pytz.timezone('US/Pacific'))
today = date.strftime('%Y%m%d')


# Script generated for node Data Catalog table
DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="glassdoorcurated",
    table_name=f"curated_{today}",
    transformation_ctx="DataCatalogtable_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=DataCatalogtable_node1,
    mappings=[
        ("date", "string", "date", "date"),
        ("companyname", "string", "companyname", "varchar"),
        ("companyrating", "double", "companyrating", "double"),
        ("revenuekey", "int", "revenuekey", "smallint"),
        ("sectorkey", "int", "sectorkey", "smallint"),
        ("industrykey", "int", "industrykey", "smallint"),
        ("sizekey", "int", "sizekey", "smallint"),
        ("typekey", "int", "typekey", "smallint"),
        ("educationkey", "int", "educationkey", "smallint"),
        ("experiencekey", "int", "experiencekey", "smallint"),
        ("companyyearfounded", "int", "companyyearfounded", "smallint"),
        ("companyage", "int", "companyage", "smallint"),
        ("easyapply", "string", "easyapply", "varchar"),
        ("citykey", "int", "citykey", "smallint"),
        ("statekey", "int", "statekey", "smallint"),
        ("minsalary", "double", "minsalary", "double"),
        ("maxsalary", "double", "maxsalary", "double"),
        ("averagesalary", "double", "averagesalary", "double"),
        ("stream", "int", "stream", "boolean"),
        ("sql", "int", "sql", "boolean"),
        ("gcp", "int", "gcp", "boolean"),
        ("scala", "int", "scala", "boolean"),
        ("dbt", "int", "dbt", "boolean"),
        ("java", "int", "java", "boolean"),
        ("azure", "int", "azure", "boolean"),
        ("aws", "int", "aws", "boolean"),
        ("batch", "int", "batch", "boolean"),
        ("spark", "int", "spark", "boolean"),
        ("python", "int", "python", "boolean"),
        ("airflow", "int", "airflow", "boolean"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://aws-glue-assets-288113613717-us-west-2/temporary/",
        "url": "jdbc:redshift://glassdoorproject.288113613717.us-west-2.redshift-serverless.amazonaws.com:5439/dev?user=<>&password=<>",
        "dbtable": "public.fact",
        "connectionName": "glassdoorredshift",
        "preactions": "CREATE TABLE IF NOT EXISTS public.fact (date DATE, companyname VARCHAR, companyrating DOUBLE PRECISION, revenuekey VARCHAR, sectorkey VARCHAR, industrykey VARCHAR, sizekey VARCHAR, typekey VARCHAR, educationkey VARCHAR, experiencekey VARCHAR, companyyearfounded VARCHAR, companyage VARCHAR, easyapply VARCHAR, citykey VARCHAR, statekey VARCHAR, minsalary DOUBLE PRECISION, maxsalary DOUBLE PRECISION, averagesalary DOUBLE PRECISION, stream BOOLEAN, sql BOOLEAN, gcp BOOLEAN, scala BOOLEAN, dbt BOOLEAN, java BOOLEAN, azure BOOLEAN, aws BOOLEAN, batch BOOLEAN, spark BOOLEAN, python BOOLEAN, airflow BOOLEAN);",
    },
    transformation_ctx="AmazonRedshift_node3",
)
job.commit()
