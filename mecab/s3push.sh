#!/bin/bash
aws s3 cp ./stock_brands_patch.xlsx s3://kabupac.system/stock_brands_patch.xlsx
aws s3 cp ./mecab_dic.xlsx s3://kabupac.system/mecab_dic.xlsx