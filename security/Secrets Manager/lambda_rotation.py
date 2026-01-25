import boto3
from botocore.exceptions import ClientError
import json
import string

"""

Step 1: if RotationEnabled is False: Break
Step 2: if token not in VersionIdsToStages: break
Step 3: if token in CURRENT: return (Already set)
step 4: if token not is PENDIND: break (not set to be rotate)
step 5: perform step createSecret, setSecret, testSecret, finishSecret

"""

import boto3
from botocore.exceptions import ClientError
import json
import logger

logger = logging.getLogger()
logger.setLevel(logging.INFO)

session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name='us-east-1'
)

def lambda_handler(event, context):
    arn = event['SecretId']
    token = event['ClientRequestToken']
    step = event['Step']

    metadata = client.describe_secret(SecretId=arn)
    if not metadata['RotationEnabled']:
        logger.log_error("Secret %s is not enabled for rotation" % arn)
        raise ValueError("Secret %s is not enabled for rotation" % arn)
    
    if token not in metadata['VersionIdsToStages']:
        logger.info("Secret version %s has no stage for rotation" % token)
        raise ValueError('Secret version %s has no stage for rotation' % token)
    if token in metadata['VersionIdsToStages']['AWSCURRENT']:
        logger.info("Secret version %s already set as AWSCURRENT" % token)
        return
    elif token not in metadata['VersionIdsToStages']['AWSPENDIND']:
        logger.info("Secret version %s is not set to be rotated" % token)
        raise ValueError('Secret version %s is not set to be rotated' % token)

    if step == "createSecret":
        create_secret(client, arn, token)   
    elif step == "setSecret":
        set_secret(client, arn, token)
    elif step == "testSecret":
        test_secret(client, arn, token):
    elif step == "finishSecret":
        finish_secret(client, arn, token)"
    else:
        raise ValueError("Invalid step parameter")


def create_secret(client, arn, token):
    pass

def set_secret(client, arn, token):
    pass

def test_secret(client, arn, token):
    pass

def finish_secret(client, arn, token):
    pass


def generate_password(length=32):
    chars = string.ascii_letters + string.digits + "!@#$%^&*()"
    return ''.join(secrets.choice(chars) for _ in range(length))

    