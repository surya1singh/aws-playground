
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
import logging
import secrets
import string

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
    print("metadata ", metadata)
    print("event",event)
    if not metadata['RotationEnabled']:
        logger.log_error("Secret %s is not enabled for rotation" % arn)
        raise ValueError("Secret %s is not enabled for rotation" % arn)
    
    versions = metadata['VersionIdsToStages']
    if token not in versions:
        logger.info("Secret version %s has no stage for rotation" % token)
        raise ValueError('Secret version %s has no stage for rotation' % token)
    if 'AWSCURRENT' in versions[token]:
        logger.info("Secret version %s already set as AWSCURRENT" % token)
        return
    elif 'AWSPENDING' not in versions[token]:
        logger.info("Secret version %s is not set to be rotated" % token)
        raise ValueError('Secret version %s is not set to be rotated' % token)

    if step == "createSecret":
        create_secret(client, arn, token)   
    elif step == "setSecret":
        set_secret(client, arn, token)
    elif step == "testSecret":
        test_secret(client, arn, token)
    elif step == "finishSecret":
        finish_secret(client, arn, token)
    else:
        raise ValueError("Invalid step parameter")




def create_secret(client, arn, token):
    """
    Make sure the current Secret exists for AWSCURRENT
    Read secret version for AWSPENDING, if that fails, put a new secret
    """
    
    current_dict = get_secret_dict(client, arn, "AWSCURRENT")
    try:
        get_secret_dict(client, arn, "AWSPENDING", token)
        logger.info("createSecret: Successfully retrieved secret for %s." % arn)
    except client.exceptions.ResourceNotFoundException:
        current_dict['password'] = generate_password()
        client.put_secret_value(SecretId=arn, ClientRequestToken=token, SecretString=json.dumps(current_dict), VersionStages=['AWSPENDING'])
        logger.info("createSecret: Successfully put secret for ARN %s and version %s." % (arn, token))


def set_secret(client, arn, token):
    """
    TO DO:
    Update MySQL crediantials

    flow:
        connect with current
             True: update password AWSPENDING
             False: connect with AWSPENDING
    """
    current_dict = get_secret_dict(client, arn, "AWSCURRENT")
    pending_dict = get_secret_dict(client, arn, "AWSPENDING", token)
    logger.info("setSecret: Successfully set password for secret arn %s." % arn)

def test_secret(client, arn, token):
    """
    TO DO:
    Connect mysql with AWSPENDING and run a query
    """
    logger.info("testSecret: Successfully signed into MySQL DB with AWSPENDING secret in %s." % arn)
    return

def finish_secret(client, arn, token):
    
    metadata = client.describe_secret(SecretId=arn)
    current_version = None
    for version in metadata["VersionIdsToStages"]:
        if "AWSCURRENT" in metadata["VersionIdsToStages"][version]:
            if version == token:
                logger.info("finishSecret: Version %s already marked as AWSCURRENT for %s" % (version, arn))
                return
            current_version = version
            break

    client.update_secret_version_stage(SecretId=arn, VersionStage="AWSCURRENT", MoveToVersionId=token, RemoveFromVersionId=current_version)
    logger.info("finishSecret: Successfully set AWSCURRENT stage to version %s for secret %s." % (token, arn))


def generate_password(length=32):
    chars = string.ascii_letters + string.digits + "!@#$%^&*()"
    return ''.join(secrets.choice(chars) for _ in range(length))


def get_secret_dict(client, arn, stage, token=None):
    if token:
        secret = client.get_secret_value(SecretId=arn, VersionId=token, VersionStage=stage)
    else:
        secret = client.get_secret_value(SecretId=arn, VersionStage=stage)
        
    secret_dict = json.loads(secret['SecretString'])
    logger.info("secret_dict",secret_dict)

    return secret_dict
    