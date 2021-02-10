#!/bin/bash
if [ $# -ne 1 ]; then
    echo '引数必要です。 TaskXXX'
    exit 1
fi
cd $(dirname $0)

FOL=`echo $1 | sed -e "s/[0-9]*$//g"`

val=$1
cd ${FOL}/$1
rm -rf __pycache__
zip -r $1.zip ./*
cd -

aws s3 cp ${FOL}/$1/$1.zip s3://kabupac.system/lambda_dev/$1.zip
aws lambda delete-function --region ap-northeast-1 \
                           --function-name $1
aws lambda create-function --region ap-northeast-1 \
                           --function-name $1 \
                           --runtime python3.6 \
                           --code S3Bucket=kabupac.system,S3Key=lambda_dev/$1.zip \
                           --role arn:aws:iam::007575903924:role/cloud9-Job001-Job001Role-NQ27F73FEWTL \
                           --handler $1.${val,,}_handler \
                           --memory-size 256 \
                           --timeout 240