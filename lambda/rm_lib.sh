#!/bin/bash
if [ $# -ne 1 ]; then
    echo '引数必要です。 TaskXXX'
    exit 1
fi

cd $(dirname $0)
cd ../requirements/

FOL=`echo $1 | sed -e "s/[0-9]*$//g"`

envdirs=()
for dpath in `find ./${FOL} -type d` ; do
    dirname=`basename $dpath`
    if [ $1 = $dirname -o '__pycache__' = $dirname ]; then
        continue
    fi
    envdirs+=($dirname)
done

cd ../lambda
rm -rf ./${FOL}/$1/common
for row in ${envdirs[@]}; do
    rm -rf ./${FOL}/$1/$row
done

rm -f ./${FOL}/$1/six.py
rm -f ./${FOL}/$1/$1.zip
