# \[Alpha] AWS Lambda feature server

**Warning**: This is an _experimental_ feature. It's intended for early testing and feedback, and could change without warnings in future releases.

## Overview

The AWS Lambda feature server is an HTTP endpoint that serves features with JSON I/O, deployed as a Docker image through AWS Lambda and AWS API Gateway. This enables users to get features from Feast using any programming language that can make HTTP requests. A [local feature server](python-feature-server.md) is also available. A remote feature server on GCP Cloud Run is currently being developed.

## Deployment

The AWS Lambda feature server is only available to projects using the `AwsProvider` with registries on S3. It is disabled by default. To enable it, `feature_store.yaml` must be modified; specifically, the `enable` flag must be on and an `execution_role_name` must be specified. For example, after running `feast init -t aws`, changing the registry to be on S3, and enabling the feature server, the contents of `feature_store.yaml` should look similar to the following:

```
project: dev
registry: s3://feast/registries/dev
provider: aws
online_store:
  region: us-west-2
offline_store:
  cluster_id: feast
  region: us-west-2
  user: admin
  database: feast
  s3_staging_location: s3://feast/redshift/tests/staging_location
  iam_role: arn:aws:iam::{aws_account}:role/redshift_s3_access_role
feature_server:
  enabled: True
  execution_role_name: arn:aws:iam::{aws_account}:role/lambda_execution_role
```

If enabled, the feature server will be deployed during `feast apply`. After it is deployed, the `feast endpoint` CLI command will indicate the server's endpoint.

## Permissions

Feast requires the following permissions in order to deploy and teardown AWS Lambda feature server:

| Permissions                                                                                                                                                                                                                                                                                                                                                                                                           | Resources                                                                                                                                                                                                                                                                                                                                                                                              |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| <p>lambda:CreateFunction</p><p>lambda:GetFunction</p><p>lambda:DeleteFunction</p><p>lambda:AddPermission</p><p>lambda:UpdateFunctionConfiguration</p>                                                                                                                                                                                                                                                                 | arn:aws:lambda:\<region>:\<account_id>:function:feast-\*                                                                                                                                                                                                                                                                                                                                               |
| <p>ecr:CreateRepository</p><p>ecr:DescribeRepositories</p><p>ecr:DeleteRepository</p><p>ecr:PutImage</p><p>ecr:DescribeImages</p><p>ecr:BatchDeleteImage</p><p>ecr:CompleteLayerUpload</p><p>ecr:UploadLayerPart</p><p>ecr:InitiateLayerUpload</p><p>ecr:BatchCheckLayerAvailability</p><p>ecr:GetDownloadUrlForLayer</p><p>ecr:GetRepositoryPolicy</p><p>ecr:SetRepositoryPolicy</p><p>ecr:GetAuthorizationToken</p> | \*                                                                                                                                                                                                                                                                                                                                                                                                     |
| <p>iam:PassRole</p>                                                                                                                                                                                                                                                                                                                                                                                                   | arn:aws:iam::\<account_id>:role/<lambda-execution-role-name>                                                                                                                                                                                                                                                                                                                                           |
| <p>apigateway:*</p>                                                                                                                                                                                                                                                                                                                                                                                                   | <p>arn:aws:apigateway:*::/apis/*/routes/*/routeresponses</p><p>arn:aws:apigateway:*::/apis/*/routes/*/routeresponses/*</p><p>arn:aws:apigateway:*::/apis/*/routes/*</p><p>arn:aws:apigateway:*::/apis/*/routes</p><p>arn:aws:apigateway:*::/apis/*/integrations</p><p>arn:aws:apigateway:*::/apis/*/stages/*/routesettings/*</p><p>arn:aws:apigateway:*::/apis/*</p><p>arn:aws:apigateway:*::/apis</p> |

The following inline policy can be used to grant Feast the necessary permissions:

```javascript
{
    "Statement": [
        {
        Action = [
          "lambda:CreateFunction",
          "lambda:GetFunction",
          "lambda:DeleteFunction",
          "lambda:AddPermission",
          "lambda:UpdateFunctionConfiguration",
        ]
        Effect = "Allow"
        Resource = "arn:aws:lambda:<region>:<account_id>:function:feast-*"
      },
      {
        Action = [
            "ecr:CreateRepository",
            "ecr:DescribeRepositories",
            "ecr:DeleteRepository",
            "ecr:PutImage",
            "ecr:DescribeImages",
            "ecr:BatchDeleteImage",
            "ecr:CompleteLayerUpload",
            "ecr:UploadLayerPart",
            "ecr:InitiateLayerUpload",
            "ecr:BatchCheckLayerAvailability",
            "ecr:GetDownloadUrlForLayer",
            "ecr:GetRepositoryPolicy",
            "ecr:SetRepositoryPolicy",
            "ecr:GetAuthorizationToken"
        ]
        Effect = "Allow"
        Resource = "*"
      },
      {
        Action = "iam:PassRole"
        Effect = "Allow"
        Resource = "arn:aws:iam::<account_id>:role/<lambda-execution-role-name>"
      },
      {
        Effect = "Allow"
        Action = "apigateway:*"
        Resource = [
            "arn:aws:apigateway:*::/apis/*/routes/*/routeresponses",
            "arn:aws:apigateway:*::/apis/*/routes/*/routeresponses/*",
            "arn:aws:apigateway:*::/apis/*/routes/*",
            "arn:aws:apigateway:*::/apis/*/routes",
            "arn:aws:apigateway:*::/apis/*/integrations",
            "arn:aws:apigateway:*::/apis/*/stages/*/routesettings/*",
            "arn:aws:apigateway:*::/apis/*",
            "arn:aws:apigateway:*::/apis",
        ]
      },
    ],
    "Version": "2012-10-17"
}
```

## Example

After `feature_store.yaml` has been modified as described in the previous section, it can be deployed as follows:

```bash
$ feast apply
10/07/2021 03:57:26 PM INFO:Pulling remote image feastdev/feature-server-python-aws:aws:
10/07/2021 03:57:28 PM INFO:Creating remote ECR repository feast-python-server-key_shark-0_13_1_dev23_gb3c08320:
10/07/2021 03:57:29 PM INFO:Pushing local image to remote 402087665549.dkr.ecr.us-west-2.amazonaws.com/feast-python-server-key_shark-0_13_1_dev23_gb3c08320:0_13_1_dev23_gb3c08320:
10/07/2021 03:58:44 PM INFO:Deploying feature server...
10/07/2021 03:58:45 PM INFO:  Creating AWS Lambda...
10/07/2021 03:58:46 PM INFO:  Creating AWS API Gateway...
Registered entity driver_id
Registered feature view driver_hourly_stats
Deploying infrastructure for driver_hourly_stats

$ feast endpoint
10/07/2021 03:59:01 PM INFO:Feature server endpoint: https://hkosgmz4m2.execute-api.us-west-2.amazonaws.com

$ feast materialize-incremental $(date +%Y-%m-%d)
Materializing 1 feature views to 2021-10-06 17:00:00-07:00 into the dynamodb online store.

driver_hourly_stats from 2020-10-08 23:01:34-07:00 to 2021-10-06 17:00:00-07:00:
100%|█████████████████████████████████████████████████████████████████| 5/5 [00:00<00:00, 16.89it/s]
```

After the feature server starts, we can execute cURL commands against it:

```bash
$ curl -X POST \                                 
    "https://hkosgmz4m2.execute-api.us-west-2.amazonaws.com/get-online-features" \
    -H "Content-type: application/json" \
    -H "Accept: application/json" \
    -d '{
        "features": [
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:acc_rate",
            "driver_hourly_stats:avg_daily_trips"
        ],
        "entities": {
            "driver_id": [1001, 1002, 1003]
        },
        "full_feature_names": true
    }' | jq
  % Total    % Received % Xferd  Average Speed   Time    Time     Time  Current
                                 Dload  Upload   Total   Spent    Left  Speed
100  1346  100  1055  100   291   3436    947 --:--:-- --:--:-- --:--:--  4370
{
  "field_values": [
    {
      "fields": {
        "driver_id": 1001,
        "driver_hourly_stats__conv_rate": 0.025330161675810814,
        "driver_hourly_stats__avg_daily_trips": 785,
        "driver_hourly_stats__acc_rate": 0.835975170135498
      },
      "statuses": {
        "driver_hourly_stats__avg_daily_trips": "PRESENT",
        "driver_id": "PRESENT",
        "driver_hourly_stats__conv_rate": "PRESENT",
        "driver_hourly_stats__acc_rate": "PRESENT"
      }
    },
    {
      "fields": {
        "driver_hourly_stats__conv_rate": 0.7595187425613403,
        "driver_hourly_stats__acc_rate": 0.1740121990442276,
        "driver_id": 1002,
        "driver_hourly_stats__avg_daily_trips": 875
      },
      "statuses": {
        "driver_hourly_stats__acc_rate": "PRESENT",
        "driver_id": "PRESENT",
        "driver_hourly_stats__avg_daily_trips": "PRESENT",
        "driver_hourly_stats__conv_rate": "PRESENT"
      }
    },
    {
      "fields": {
        "driver_hourly_stats__acc_rate": 0.7785481214523315,
        "driver_hourly_stats__conv_rate": 0.33832859992980957,
        "driver_hourly_stats__avg_daily_trips": 846,
        "driver_id": 1003
      },
      "statuses": {
        "driver_id": "PRESENT",
        "driver_hourly_stats__conv_rate": "PRESENT",
        "driver_hourly_stats__acc_rate": "PRESENT",
        "driver_hourly_stats__avg_daily_trips": "PRESENT"
      }
    }
  ]
}
```
