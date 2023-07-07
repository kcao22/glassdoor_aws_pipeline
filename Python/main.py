# K.Cao 

# Imports
import configparser
import datetime
import json
from aws_helper_function import create_session
from scraping import collect_all_data

# Write to log text file
file = open(r'D:\Documents\Data Projects\glassdoor_aws_pipeline\logs.txt', 'a')
file.write(f'{datetime.datetime.now()} - Script is running.\n')

# Parser to get config credentials and other private information
parser = configparser.ConfigParser()
parser.read_file(open(r'D:\Documents\Data Projects\glassdoor_aws_pipeline\credentials.config'))
# AWS credentials
bucket_name = parser.get('AWS', 'bucket_name')
aws_access_key = parser.get('AWS', 'aws_access_key')
aws_secret_key = parser.get('AWS', 'aws_secret_key')
aws_region = parser.get('AWS', 'region')

# Create boto3 session
session = create_session()
s3 = session.client('s3')

file.write(f'{datetime.datetime.now()} - Begin scraping.\n')
# Scrape data
all_data = collect_all_data(test=False)

file.write(f'{datetime.datetime.now()} - Placing JSON into S3.\n')
# Put JSON object of raw data into S3 raw
file_name = f'raw/{datetime.datetime.now().strftime("Raw_%Y%m%d")}.json'
s3.put_object(
    Bucket=f'{bucket_name}',
    Key=file_name,
    Body=json.dumps(all_data)
)

file.write(f'{datetime.datetime.now()} - Script completed running.\n\n')

