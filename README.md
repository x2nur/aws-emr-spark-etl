
## Apache Spark processing on AWS EMR (Serverless) with AWS Step Functions orchestration and CDK deployment

<img src="/data_pipeline_scheme.svg" width=70%>

This project utilizes the following AWS services:

- CloudFormation (CDK)
- Lambda
- IAM
- Step Functions
- EMR Serverless
- S3
- Glue Data Catalog
- Event Bridge

#### Usage

Preinstalled tools:
- CDK
- AWS CLI (optional)

After cloning:

```
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip install -r requirements-dev.txt
```

Initializing: 
```
$ cdk bootstrap
```

Deploy:

```
$ cdk deploy
```
