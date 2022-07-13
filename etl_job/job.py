import boto3
import json
import pandas as pd
import numpy as np

bucket, dion, sawyer = "tsan-bucket-trial", "Dion-DISTRIBUTOR_R3_MASTER-2022-07-04_19_00_00.json", "Sawyer-DISTRIBUTOR_R3_MASTER-2022-07-04_19_00_00.json"

s3_obj = boto3.resource('s3')
s3_client = boto3.client('s3')

def getDF(bucketName, fileName):
    
    response = s3_client.get_object(Bucket=bucketName, Key=fileName)
    data = response['Body'].read()
    data = json.loads(data)
    df = pd.DataFrame(data['data'])
    return df




dion_df = getDF(bucket, dion)
sawyer_df = getDF(bucket, sawyer)

print(dion_df.head())
print(sawyer_df.head())