# -*- coding:utf-8 -*-
import boto3

def queue001_handler(event, context):
    client = boto3.client('batch')

    job_name = 'queue-Job010'
    job_queue = "arn:aws:batch:ap-northeast-1:007575903924:job-queue/Job010_create_price_history"
    job_definition = "Job010_create_price_history:1"

    response = client.submit_job(
        jobName=job_name,
        jobQueue=job_queue,
        jobDefinition=job_definition
    )
    print(response)
    return 0
