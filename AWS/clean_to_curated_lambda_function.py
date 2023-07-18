import awswrangler as wr
import boto3
import json
from urllib.parse import unquote_plus


def lambda_handler(event, context):
    session = boto3.Session()
    glue = session.client('glue')
    res = glue.start_job_run(
            JobName='CleanToCurated'
        )
    return res