
# Apache Spark processing on AWS EMR (Serverless) with AWS Step Functions orchestration and CDK deployment


![data pipeline scheme](/data_pipeline_scheme.svg)


## Usage


Preinstalled tools:
- CDK
- AWS CLI (optional)


After cloning:

```
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip install -r requirements-dev.txt
```

Deploy:

```
$ cdk deploy
```