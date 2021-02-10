#!/bin/bash
cd $(dirname $0)
cd ../requirements
FOLDER=`echo $1 | sed -e "s/[0-9]*$//g"`

rm -rf ./${FOLDER}
pip install -r ${FOLDER}.txt -t ./${FOLDER}
