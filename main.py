import boto3
import sys
from datetime import date

## variable
BUCKET_NAME = 'XXX'
ACCOUNT_ID = 'XXX'
## ACCOUNT_ID = '999999999999'
TOPIC_ARN = 'XXX'
## TOPIC_ARN = 'arn:aws:sns:(region):(ACCOUNT_ID):(Topic_Name)'

def named_csvfile(ID):
    today = date.today()
    if today.month == 1:
        named = ID + "-aws-billing-csv-" + str(today.year-1).zfill(4) + "-12.csv"
    else:
        named = ID + "-aws-billing-csv-" + str(today.year).zfill(4) + "-" + str(today.month-1).zfill(2) + ".csv"
    return named

def publish_sns(records):
    sns = boto3.client('sns')
    
    try:
        response = sns.publish(
            TopicArn=TOPIC_ARN,
            Subject="Inform of AWS Budget by Lambda Function",
            Message="先月のAWS利用料金をご連絡します。\n\n" + records.strip() + "$ となります。"
        )
        print("メールを送信しました。処理を終了します。")
    except:
        print("TopicARNが間違っています。処理を中断します。")
        sys.exit(1)

def lambda_handler(event, context):
    client = boto3.client('s3')
    KEY_NAME = named_csvfile(ACCOUNT_ID)

    try:
        responce = client.select_object_content(
            Bucket = BUCKET_NAME,
            Key = KEY_NAME,
            Expression = 'select TotalCost from s3object s where RecordType like \'%InvoiceTotal%\'',
            ExpressionType = 'SQL',
            InputSerialization = {
                'CSV': {
                    'FileHeaderInfo': 'USE',
                    'FieldDelimiter': ',',
                    'RecordDelimiter': '\n'
                },
                'CompressionType': 'NONE'
            },
            OutputSerialization = {
                'CSV': {
                    'FieldDelimiter': ',',
                    'RecordDelimiter': ''
                }
            }
        )
    except:
        print("アカウントIDかS3バケットネームが間違っています。処理を中断します。")
        sys.exit(1)
    
    for event in responce[ 'Payload' ]:
        if 'Records' in event:
            records = event[ 'Records' ][ 'Payload' ].decode( 'utf-8' )
    
    publish_sns(records)
