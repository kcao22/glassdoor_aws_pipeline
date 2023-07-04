# K.Cao 

# Imports
import configparser
import datetime
import json
from aws_helper_function import create_session
from scraping import collect_all_data

# Parser to get config credentials and other private information
parser = configparser.ConfigParser()
parser.read_file(open('../credentials.config'))
# AWS credentials
bucket_name = parser.get('AWS', 'bucket_name')
aws_access_key = parser.get('AWS', 'aws_access_key')
aws_secret_key = parser.get('AWS', 'aws_secret_key')
aws_region = parser.get('AWS', 'region')

# Create boto3 session
session = create_session()
s3 = session.client('s3')

# Scrape data
all_data = collect_all_data(test=False)

# Put JSON object of raw data into S3 raw
file_name = f'{datetime.datetime.now().strftime("%Y%m%d-%H%M%S")}-raw'
s3.put_object(
    Bucket=f'{bucket_name}/raw/',
    Key=file_name,
    Body=json.dumps(all_data)
)