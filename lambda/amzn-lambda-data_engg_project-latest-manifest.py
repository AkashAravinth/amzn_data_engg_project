import json
import boto3
from datetime import datetime
import time

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    
    try:
        # logic to retrieve the latest file
        path_except_filename="s3://amzn-s3-data-engg-project-final-bucket/"

        response=s3_client.list_objects_v2(Bucket='amzn-s3-data-engg-project-final-bucket',Prefix='manifests/')

        all_files=response['Contents']
        latest_file=sorted(all_files, key=lambda x: x['LastModified'])[-1]['Key']

        manifest_path =path_except_filename+latest_file

        return {
            "manifest_path":manifest_path
        }
        
    except Exception as e:
        return {
            "error": str(e),
            "manifest_path": None
        }