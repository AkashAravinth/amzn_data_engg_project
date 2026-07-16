import json
import boto3
from io import StringIO
import pandas as pd
import os
from datetime import datetime, timezone

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('amzn_data_engg_project-dynamoDB')

#using env variable for fetching data
from_bucket_name = os.environ.get('FROM_BUCKET')
dest_bucket_name = os.environ.get('DEST_BUCKET')
status_indicator_folder_name = os.environ.get('STATUS_INDICATOR')
file_schema_path=os.environ.get('FILE_SCHEMA_FILE_PATH')

def evaluate_check(key, value) -> bool:
    if isinstance(value, bool):
        return value                # True = pass, False = fail
    elif isinstance(value, list):
        return len(value) == 0      # empty list = pass
    return False

def write_DynamoDB(bucket_name, file_name, row_count, flag_for_historical_or_incremental, status):
    try:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
        record_id = f"{file_name}_{timestamp}"

        table.put_item(
            Item={
                'id': record_id,
                'bucket_name': bucket_name,
                'file_name': file_name,
                'row_count': row_count,
                'flag_for_historical_or_incremental': flag_for_historical_or_incremental,
                'status': status,
                'timestamp': timestamp
            }
        )

        print(f"DQ check record written successfully: {record_id}")

        return {
            'success': True,
            'id': record_id,
            'message': 'Record written successfully'
        }

    except Exception as e:
        print(f"Unexpected error writing DQ check record: {str(e)}")

        return {
            'success': False,
            'id': None,
            'message': str(e)
        }

def get_file_name(load_type):
    prefix = f"{load_type}/"
    list_response=s3_client.list_objects_v2(Bucket=from_bucket_name, Prefix=prefix)
    
    has_actual_files = False
    if 'Contents' in list_response:
        for obj in list_response['Contents']:
            # Skip folder placeholders (keys ending with '/') and zero-byte files
            if not obj['Key'].endswith('/') and obj['Size'] > 0:
                has_actual_files = True
                break

    if has_actual_files:
        all_files=list_response['Contents']
        latet_file_with_prefix=sorted(all_files, key=lambda x: x['LastModified'])[-1]['Key']
        file_name = latet_file_with_prefix[len(prefix):] if latet_file_with_prefix.startswith(prefix) else latet_file_with_prefix

        return {'success': True, 'file_name':file_name}
    else:
        return {'success': False, 'message': str(e)}

def dq_checks(df_raw_data,row_count):
    dq_results={}

    # row count check
    dq_results['row_count']=row_count>0

    # missed columns check

    response = s3_client.get_object(Bucket=from_bucket_name, Key=file_schema_path)
    content = response['Body'].read().decode('utf-8')

    # Split into list, one entry per row/line
    lines = content.splitlines()

    # strip whitespace and remove empty lines
    expected_cols = [line.strip() for line in lines if line.strip()]

    dq_results['missing_columns']=[col for col in df_raw_data.columns if col not in expected_cols]

    # is duplicate items present in the primary key colm
    if 'match_id' in df_raw_data.columns:
        dq_results['duplicate_ids']= not df_raw_data['match_id'].duplicated().any()

    # buisness rule checks if any. ex range checks like a certain column between 10 to 100

    # freshness or time line chec, to ensure the new data arrived on the expected time.

    # data type validation check, if there is a date column, to check whether all the data follows the expected format

    # if a cloumn should have only among one of these data, then it should be seen whether any other data is present.

    return dq_results

def lambda_handler(event, context):
    # load-type
    load_type = event['load_type']

    # To empty the folder lambda-DQ-check-status-indicator/
    s3_client.delete_object(Bucket=from_bucket_name, Key=f"{status_indicator_folder_name}/0")

    # get the filename and the load to df_raw_data
    result=get_file_name(load_type)
    if not result['success']:
        # This is what actually triggers a Step Functions Catch/Retry
        raise Exception(f"Failed to write DQ record: {result['message']}")
    file_name=result['file_name']

    s3_object=s3_client.get_object(Bucket=from_bucket_name,Key=f"{load_type}/{file_name}")
    body=s3_object['Body'].read().decode('utf-8')
    df_raw_data=pd.read_csv(StringIO(body))
    
    # used in many functions
    row_count=len(df_raw_data)

    # DQ check call
    dq_results=dq_checks(df_raw_data,row_count)

    # to define the final status of the data load based on the DQ checks defined
    dq_pass_fail = {key: evaluate_check(key, val) for key, val in dq_results.items()}

    status=all(dq_pass_fail.values())

    # write to staging bucket
    if status:
        copy_source = {'Bucket': from_bucket_name, 'Key': f"{load_type}/{file_name}"}
        if load_type == 'historical_data':
            # if historical, then copy and put under the historical in staging bucket
            s3_client.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket_name,
               Key=f"{load_type}/staged_{file_name}"
            )
        else:
            # if incremental, then move to the incremental in staging bucket
            s3_client.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket_name,
                Key=f"{load_type}/staged_{file_name}"
            )

            s3_client.delete_object(Bucket=from_bucket_name, Key=f"{load_type}/{file_name}")
        

        # add the 0 byte file to the folder lambda-DQ-check-status-indicator/ in raw bucket
        s3_client.put_object(Bucket=from_bucket_name, Key=f"{status_indicator_folder_name}/0", Body="")

    
    # code for populating the DynamoDB
    result = write_DynamoDB(
        bucket_name=from_bucket_name,
        file_name=file_name,
        row_count=row_count,
        flag_for_historical_or_incremental=True if load_type == 'historical_data' else False,
        status=status
    )
    if not result['success']:
        # This is what actually triggers a Step Functions Catch/Retry
        raise Exception(f"Failed to write DQ record: {result['message']}")

    return {
        'statusCode': 200,
        'body': json.dumps('DQ checks'),
        'load_type' : load_type
    }
