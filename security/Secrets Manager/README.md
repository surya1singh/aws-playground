This module implements automatic password rotation for an RDS MySQL database using
AWS Secrets Manager and a custom Lambda rotation function.

---

## Architecture

Secrets Manager → Rotation Lambda → RDS MySQL  
Glue Job → Secrets Manager (read-only)

---

## Rotation Flow

1. Secrets Manager triggers Lambda
2. Lambda generates a new password
3. Updates MySQL user password
4. Validates new credentials
5. Promotes AWSPENDING → AWSCURRENT

---

## Security Design

- Secret access restricted via resource policy
- Only:
  - Rotation Lambda (read/write)
  - Glue Job (read-only)
- No human access
- Password never logged or stored in plaintext

---

## IAM & Permissions

- Lambda execution role:
  - Secrets Manager access (scoped)
  - CloudWatch logging
- Secrets Manager resource policy blocks all other principals

---

## Deployment Notes

- Lambda runs inside VPC
- The security group allows access only to RDS
- pymysql provided via Lambda Layer
- Rotation interval configurable (e.g., 30 days)

---

## Failure Scenarios

- DB unreachable → rotation fails, secret unchanged
- Password update fails → rollback safe
- Glue job always reads AWSCURRENT

---


## Files

-- lambda access policy.json -> default policy for cloudwatch + secretsmanager for one secret only + KMS 
-- Secret manager Resource manager.json -> give access to lamdba + deny all other access
-- lambda_rotation.py -> lambda code to rotate credentials


## To do:
1. Create an RDS instance
2. Access rds from lambda
3. Use glue to get access credentials from SM and access glue
   Add glue in the SM resource manager policy
   Give glue to access only one secret.
4. Update all policies to have only minimum access.
