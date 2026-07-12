import json
import boto3
from io import StringIO
import pandas as pd
import os

s3_client = boto3.client('s3')

def evaluate_check(key, value) -> bool:
    if isinstance(value, bool):
        return value                # True = pass, False = fail
    elif isinstance(value, list):
        return len(value) == 0      # empty list = pass
    return False

def lambda_handler(event, context):
    # s3 bucket and file details from event invocation
    s3_bucket_name= event['detail']['bucket']['name']
    s3_file_name=event['detail']['object']['key']

    #using env variable for fetching destination s3 bucket
    dest_bucket_name = os.environ.get('DEST_BUCKET')

    s3_object=s3_client.get_object(Bucket=s3_bucket_name,Key=s3_file_name)
    body=s3_object['Body'].read().decode('utf-8')
    df_raw_data=pd.read_csv(StringIO(body))

    # DQ checks region start
    dq_results={}

    # row count check
    row_count=len(df_raw_data)
    dq_results['row_count']=row_count>0

    # missed columns check

    response = s3_client.get_object(Bucket=s3_bucket_name, Key=os.environ.get('FILE_SCHEMA_FILE_PATH'))
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


    # DQ check region ends
    dq_pass_fail = {key: evaluate_check(key, val) for key, val in dq_results.items()}

    # to define the final status of the data load based on the DQ checks defined
    status=all(dq_pass_fail.values())

    load_type="historical_data"
    # logic to decide the load type
    response=s3_client.list_objects_v2(Bucket=dest_bucket_name, Prefix=load_type+"/")
    
    print(response['Contents'])

    has_actual_files = False
    if 'Contents' in response:
        for obj in response['Contents']:
            # Skip folder placeholders (keys ending with '/') and zero-byte files
            if not obj['Key'].endswith('/') and obj['Size'] > 0:
                has_actual_files = True
                break

    isHistorical= not has_actual_files

    if isHistorical:
        load_type="historical_data"
        print("historical_data")
    else:
        load_type="incremental_data"
        print("incremental_data")

    if status:
        # write to staging bucket
        csv_buffer=StringIO()
        df_raw_data.to_csv(csv_buffer,index=False)
        s3_client.put_object(Bucket=dest_bucket_name,Key=f"{load_type}/staged_{s3_file_name}",Body=csv_buffer.getvalue())
        print("write to staging bucket")
    else:
        failed_checks = {k: v for k, v in dq_pass_fail.items() if not v}
        raise Exception(json.dumps({
        "message": "DQ checks failed",
        "failed_checks": failed_checks,
        }))

    return {
        'statusCode': 200,
        'body': json.dumps('DQ checks'),
        'load_type' : load_type
    }
