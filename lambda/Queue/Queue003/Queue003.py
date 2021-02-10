# -*- coding:utf-8 -*-
import boto3

def queue003_handler(event, context):
    client = boto3.client('batch')

    job_name = 'queue-Job015'
    job_queue = "arn:aws:batch:ap-northeast-1:007575903924:job-definition/Job015_price_rise_fall:1"
    job_definition = "Job015_price_rise_fall:1"

    response = client.submit_job(
        jobName=job_name,
        jobQueue=job_queue,
        jobDefinition=job_definition
    )
    print(response)
    return 0
