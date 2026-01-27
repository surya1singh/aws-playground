import boto3
import json
import uuid

dynamo = boto3.resource('dynamodb')
table = dynamo.Table('notes')

def lambda_handler(event, context):
    print(get_items())
    method = event["httpMethod"]

    if method == "GET":
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"items": get_items()})
        }
    elif method == "POST":
        note_id = str(uuid.uuid4())
        note = json.loads(event["body"])["note"]
        table.put_item(Item={"id":note_id,"note":note})
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"items": get_items()})
        }


    
def get_items():
    response = table.scan()
    items = []
    for i in response['Items']:
        item = {"note":i['note']}
        items.append(item)
    return items