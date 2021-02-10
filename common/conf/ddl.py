DDL = {
    "stock_brands": {
        'TableName': 'stock_brands',
        'KeySchema': [
            {"AttributeName": 'ccode', "KeyType": 'HASH'},
        ],
        "AttributeDefinitions": [
            {"AttributeName": 'ccode', "AttributeType": 'S'},
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    },
############################################################################
    "twitter_friends": {
        'TableName': 'twitter_friends',
        'KeySchema': [
            {"AttributeName": 'id_str', "KeyType": 'HASH'},
        ],
        "AttributeDefinitions": [
            {"AttributeName": 'id_str', "AttributeType": 'S'},
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 3}
    },
############################################################################
    "stock_brands_high_low": {
        'TableName': 'stock_brands_high_low',
        'KeySchema': [
            {"AttributeName": 'ccode', "KeyType": 'HASH'},
            {'AttributeName': 'date', 'KeyType': 'RANGE'}
        ],
        "AttributeDefinitions": [
            {"AttributeName": 'ccode', "AttributeType": 'S'},
            {'AttributeName': 'date', 'AttributeType': 'S'}
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
    },
############################################################################
    "stock_thema_nm": {
        'TableName': 'stock_thema_nm',
        'AttributeDefinitions': [
            {'AttributeName': 'nm', 'AttributeType': 'S'},
        ],
        'KeySchema': [
            {"AttributeName": 'nm',"KeyType": 'HASH'},
        ],
        "ProvisionedThroughput": { "ReadCapacityUnits": 5, "WriteCapacityUnits": 1},
    },
############################################################################
    "stock_thema_ccode": {
        'TableName': 'stock_thema_ccode',
        'AttributeDefinitions': [
            {'AttributeName': 'ccode', 'AttributeType': 'S'},
        ],
        'KeySchema': [
            {"AttributeName": 'ccode', "KeyType": 'HASH'},
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 1},
    },
############################################################################
    "tweet": {
        'TableName': 'tweet',
        'AttributeDefinitions': [
            {'AttributeName': 'id_str', 'AttributeType': 'S'}
        ],
        'KeySchema': [
            {"AttributeName": 'id_str', "KeyType": 'HASH'}
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5, }
    },
############################################################################
    "stock_identify": {
        'TableName': 'stock_identify',
        'AttributeDefinitions': [
            {'AttributeName': 'nm', 'AttributeType': 'S'},
            {'AttributeName': 'ccode', 'AttributeType': 'S'}
        ],
        'KeySchema': [
            {"AttributeName": 'nm',"KeyType": 'HASH'},
            {'AttributeName': 'ccode', 'KeyType': 'RANGE'}
        ],
        "ProvisionedThroughput": { "ReadCapacityUnits": 3, "WriteCapacityUnits": 3},
    },
############################################################################
    "stock_brands_patch": {
        'TableName': 'stock_brands_patch',
        'AttributeDefinitions': [
            {'AttributeName': 'nm', 'AttributeType': 'S'},
            {'AttributeName': 'ccode', 'AttributeType': 'S'}
        ],
        'KeySchema': [
            {"AttributeName": 'nm',"KeyType": 'HASH'},
            {'AttributeName': 'ccode', 'KeyType': 'RANGE'}
        ],
        "ProvisionedThroughput": { "ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    },
############################################################################
    "stock_report": {
        'TableName': 'stock_report',
        'AttributeDefinitions': [
            {'AttributeName': 'ccode', 'AttributeType': 'S'}
        ],
        'KeySchema': [
            {"AttributeName": 'ccode', "KeyType": 'HASH'}
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5, }
    },
############################################################################
    "stock_report_pages": {
        'TableName': 'stock_report_pages',
        'AttributeDefinitions': [
            {'AttributeName': 'page', 'AttributeType': 'S'}
        ],
        'KeySchema': [
            {"AttributeName": 'page', "KeyType": 'HASH'}
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5, }
    },
############################################################################
    "stock_report_list_pages_1": {
        'TableName': 'stock_report_list_pages_1',
        'AttributeDefinitions': [
            {'AttributeName': 'p', 'AttributeType': 'S'}
        ],
        'KeySchema': [
            {"AttributeName": 'p', "KeyType": 'HASH'}
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5, }
    },
############################################################################
    "stock_report_list_pages_2": {
        'TableName': 'stock_report_list_pages_2',
        'AttributeDefinitions': [
            {'AttributeName': 'p', 'AttributeType': 'S'}
        ],
        'KeySchema': [
            {"AttributeName": 'p', "KeyType": 'HASH'}
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5, }
    },
############################################################################
    "stock_price_now": {
        'TableName': 'stock_price_now',
        'AttributeDefinitions': [
            {'AttributeName': 'cd', 'AttributeType': 'S'},
        ],
        'KeySchema': [
            {"AttributeName": 'cd', "KeyType": 'HASH'},
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 10},
    },
############################################################################
    "stock_price_history": {
        'TableName': 'stock_price_history',
        'AttributeDefinitions': [
            {'AttributeName': 'cd', 'AttributeType': 'S'},
            {'AttributeName': 'd', 'AttributeType': 'S'}
        ],
        'KeySchema': [
            {"AttributeName": 'cd',"KeyType": 'HASH'},
            {'AttributeName': 'd', 'KeyType': 'RANGE'}
        ],
        "ProvisionedThroughput": { "ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    },
############################################################################
    "condition": {
        'TableName': 'condition',
        'AttributeDefinitions': [
            {'AttributeName': 'key', 'AttributeType': 'S'},
        ],
        'KeySchema': [
            {"AttributeName": 'key', "KeyType": 'HASH'},
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    },
############################################################################
    "stock_ir": {
        'TableName': 'stock_ir',
        'AttributeDefinitions': [
            {'AttributeName': 'cd', 'AttributeType': 'S'},
            {'AttributeName': 'd', 'AttributeType': 'S'}
        ],
        'KeySchema': [
            {"AttributeName": 'cd',"KeyType": 'HASH'},
            {'AttributeName': 'd', 'KeyType': 'RANGE'}
        ],
        "ProvisionedThroughput": { "ReadCapacityUnits": 5, "WriteCapacityUnits": 2},
    },
############################################################################
    "stock_edinet": {
        'TableName': 'stock_edinet',
        'KeySchema': [
            {"AttributeName": 'ccode', "KeyType": 'HASH'},
        ],
        "AttributeDefinitions": [
            {"AttributeName": 'ccode', "AttributeType": 'S'},
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 2}
    },
############################################################################
    "err_value": {
        'TableName': 'err_value',
        "AttributeDefinitions": [
            {"AttributeName": 'key', "AttributeType": 'S'},
            {'AttributeName': 'd', 'AttributeType': 'S'}
        ],
        'KeySchema': [
            {"AttributeName": 'key', "KeyType": 'HASH'},
            {'AttributeName': 'd', 'KeyType': 'RANGE'}
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
    },
############################################################################
    "twitter_friends_sum": {
        'TableName': 'twitter_friends_sum',
        'KeySchema': [
            {"AttributeName": 'uid', "KeyType": 'HASH'},
        ],
        "AttributeDefinitions": [
            {"AttributeName": 'uid', "AttributeType": 'S'},
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 3}
    },
############################################################################
    "stock_brands_rise_fall": {
        'TableName': 'stock_brands_rise_fall',
        'KeySchema': [
            {"AttributeName": 'ccode', "KeyType": 'HASH'},
            {'AttributeName': 'date', 'KeyType': 'RANGE'}
        ],
        "AttributeDefinitions": [
            {"AttributeName": 'ccode', "AttributeType": 'S'},
            {'AttributeName': 'date', 'AttributeType': 'S'}
        ],
        "ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 2}
    },
}
