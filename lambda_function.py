# -*- coding: utf-8 -*-
"""
Created on Thu Mar 16 11:23:00 2023

@author: egenc
"""
import boto3
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

def lambda_handler(event, context):
    s3 = boto3.client("s3")
    connect_str = os.getenv('CONNECT_STR')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    records = event["Records"]
    
    print("Received records from s3 to update" + str(len(records)))
    
    for record in records:
        key = record["s3"]["object"]["key"]
        bucket_name = record["s3"]["bucket"]["name"]
        print("Processing object with key:" + str(key))
        file = s3.get_object(Bucket=bucket_name, Key=key)
        itr = file["Body"].iter_chunks()
        azure_container_client = blob_service_client.get_container_client(container=bucket_name)
        azure_container_client.upload_blob(name=key, data=itr)
        print("Object " + key + " transferred to azure successfully")
        
    return print("Done succesfully!")
        


