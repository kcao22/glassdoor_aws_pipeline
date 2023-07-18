import boto3
import configparser

def create_session():
    '''
    RETURNS:
        boto3 session used for creating clients. Defaults to AdminUser IAM user and correspondong credentials along with us-west-2 region.
    '''
    # Parser to get config credentials and other private information
    parser = configparser.ConfigParser()
    parser.read_file(open(r'D:\Documents\Data Projects\glassdoor_aws_pipeline\credentials.config'))
    # AWS credentials
    key = parser.get('AWS', 'aws_access_key')
    secret = parser.get('AWS', 'aws_secret_key')
    aws_region = parser.get('AWS', 'region')

    # Instantiate boto3 s3 API client
    session = boto3.Session(
        aws_access_key_id=key,
        aws_secret_access_key=secret,
        region_name=aws_region
    )
    return session