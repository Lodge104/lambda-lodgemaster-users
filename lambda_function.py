import json
import boto3
import os
import codecs
import sys
import urllib3
import io

http = urllib3.PoolManager()

dynamodb = boto3.resource('dynamodb')

client = boto3.client('dynamodb')

tableName = os.environ['table']

p1 = client.get_item(
    Key={
        'name': {
            'S': 'XSRF-TOKEN',
        },
    },
    TableName='lodge104-keys',
)
p2 = client.get_item(
    Key={
        'name': {
            'S': 'ai_session',
        },
    },
    TableName='lodge104-keys',
)
p3 = client.get_item(
    Key={
        'name': {
            'S': 'OA.LM.Lodge.Auth',
        },
    },
    TableName='lodge104-keys',
) 
p4 = client.get_item(
    Key={
        'name': {
            'S': 'ai_user',
        },
    },
    TableName='lodge104-keys',
) 
url = "https://lodgemaster-client.oa-bsa.org/api/users"

print(p1)

headers = {
'Cookie': p1['Item']['name']['S'] + '=' + p1['Item']['value']['S'] + '; ' + p2['Item']['name']['S'] + '=' + p2['Item']['value']['S'] + '; ' + p3['Item']['name']['S'] + '=' + p3['Item']['value']['S'] + '; ' + p4['Item']['name']['S'] + '=' + p4['Item']['value']['S'] + ';',
'X-Xsrf-Token': p1['Item']['value']['S']
}

def lambda_handler(event, context):
   #get() does not store in memory
   try:
      table = dynamodb.Table(tableName)
   except Exception as error:
      print(error)
      print("Error loading DynamoDB table. Check if table was created correctly and environment variable.")

   #DictReader is a generator; not stored in memory
   obj = http.request('GET', url, headers=headers)
   body = obj.data
   content = json.loads(body)

   try:
       with table.batch_writer() as batch:
           for item in content:
                print(item)
                batch.put_item(Item=item)

   except Exception as error:
        print(error)
        print("Error executing batch_writer")