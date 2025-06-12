import boto3
import json
import pandas as pd

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # TODO implement
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    print(source_bucket)
    print(object_key)

    target_bucket = 'data-pipeline-transformed-zillow-data'
    target_file_name = object_key[:-5]
    print(target_file_name)

    waiter = s3_client.get_waiter('object_exists')
    waiter.wait(Bucket=source_bucket, Key=object_key)

    response = s3_client.get_object(Bucket=source_bucket, Key=object_key)
    print(response)

    data = response['Body'].read().decode('utf-8')
    
    json_data = json.loads(data)
    print(json_data)
    f = []
    for i in json_data["data"]:
        f.append(i)
    df = pd.DataFrame(f)
    selected_columns = ['zpid', 'isShowcaseListing', 'isFeatured', 'propertyType', 'bathrooms', 'bedrooms', 'daysOnZillow']  
    df = df[selected_columns]
    print(df)

    csv_data = df.to_csv(index=False)
    bucket_name = target_bucket
    object_key = f"{target_file_name}.csv"
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=csv_data)
    print(f"CSV file '{object_key}' uploaded to S3 bucket '{bucket_name}'")


    return {
        'statusCode': 200,
        'body': json.dumps('Transformation Completed')
    }
