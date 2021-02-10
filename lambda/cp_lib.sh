#!/bin/bash
if [ $# -ne 1 ]; then
    echo '引数必要です。 TaskXXX'
    exit 1
fi
cd $(dirname $0)

FOL=`echo $1 | sed -e "s/[0-9]*$//g"`

cp -r ../common ./${FOL}/$1
cp -r ../requirements/${FOL}/* ./${FOL}/$1
