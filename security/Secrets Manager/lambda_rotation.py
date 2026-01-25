import boto3
from botocore.exceptions import ClientError
import json
import string

"""

Step 1: Generate a new password and set it to the pending password in SM
Step 2:
cases
1. The current password stored in the SM is the correct password
    a. update MySQL password to pending password
    b. update SM (previous = current, current = pending, pending to Null)
2. The current password stored in the SM is the incorrect password
    a. pending is correct
        i. update SM (previous = current, current = pending, pending to Null)
    b. previous is correct
        i. update MySQL password to pending password
        ii. update SM (current = pending, pending to Null)

"""


def lambda_handler(event, context):
    secret_name = "test/secretsmanager"
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    secret_value_response = get_secret(secret_name)

    secret = get_secret_value_response['SecretString']
    print(get_secret_value_response)


def generate_password(length=32):
    chars = string.ascii_letters + string.digits + "!@#$%^&*()"
    return ''.join(secrets.choice(chars) for _ in range(length))


def get_secret(secret_name):
    try:
        client.get_secret_value(secretId=secret_name)
    except ClientError as e:
        raise e

    