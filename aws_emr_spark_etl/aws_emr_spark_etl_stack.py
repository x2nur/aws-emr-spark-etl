from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_lambda as _lambda,
    aws_stepfunctions as sf,
    aws_dynamodb as dynamo,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as event_targets
)
from constructs import Construct


TABLE_NAME = "etl-metadata"
BUCKET_NAME = "sales-etl-202411022224"

class AwsEmrSparkEtlStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        
        # ========== Assets bucket 

        data_bucket = s3.Bucket(
            self, 
            BUCKET_NAME,
            auto_delete_objects=True, # DEV
            removal_policy=RemovalPolicy.DESTROY, # DEV
            bucket_name=BUCKET_NAME,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL)

        s3deploy.BucketDeployment(
            self, 
            "DeployAssets",
            destination_bucket=data_bucket,
            sources=[s3deploy.Source.asset("./assets/bucket")])
        
        
        # ========== EMR role 

        emrRole = iam.Role(
            self, 
            "EmrSalesETLRole",
            assumed_by=iam.ServicePrincipal("emr-serverless.amazonaws.com")) #type: ignore

        emrRole.add_to_policy(iam.PolicyStatement(
            resources=["*"], 
            actions=[
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
            ]
        ))

        emrRole.add_to_policy(iam.PolicyStatement(
            actions=["glue:*"],
            resources=["*"]
        ))

        data_bucket.grant_read_write(emrRole)
        

        # ========== DynamoDB Table - for ETL metadata
        
        tab = dynamo.Table(
            self,
            TABLE_NAME,
            removal_policy=RemovalPolicy.DESTROY, # DEV
            partition_key=dynamo.Attribute(name="etl-name", type=dynamo.AttributeType.STRING),
            write_capacity=1,
            read_capacity=1)
        

        # ========== Lambda - Check for updates

        lmb_chk_upd = _lambda.Function(
            self, 
            "CheckForUpdates", 
            runtime=_lambda.Runtime.PYTHON_3_12,
            code=_lambda.AssetCode.from_asset("./assets/lambda/check_for_updates"),
            handler="function.handler",
            environment={
                "ETL_METADATA_TAB": TABLE_NAME,
                "CSV_PATH": data_bucket.s3_url_for_object("data/raw/sales"),
            })

        tab.grant_read_data(lmb_chk_upd)


        # ========== Lambda - Commit ETL changes

        lmb_cmt_etl = _lambda.Function(
            self, 
            "CommitETL", 
            runtime=_lambda.Runtime.PYTHON_3_12,
            code=_lambda.AssetCode.from_asset("./assets/lambda/commit_etl"),
            handler="function.handler",
            environment={
                "ETL_METADATA_TAB": TABLE_NAME,
            })
        
        tab.grant_read_write_data(lmb_cmt_etl)


        # ========== Step Functions Process 

        sm = sf.StateMachine(
            self, 
            "Sales-ETL", 
            definition_body=sf.DefinitionBody.from_file('./assets/sales-etl.asl.json'),
            )
        
        sm.add_to_role_policy(iam.PolicyStatement(
            resources=["*"], 
            actions=[
                "emr-serverless:CreateApplication",
                "emr-serverless:GetApplication",
                "emr-serverless:DeleteApplication",
                "emr-serverless:StartApplication",
                "emr-serverless:StopApplication"
            ]
        ))

        sm.add_to_role_policy(iam.PolicyStatement(
            resources=[
                "*"
                # f"arn:aws:events:{self.region}:{self.account}:rule/StepFunctionsGetEventsForEMRServerlessApplicationRule",
                # f"arn:aws:events:{self.region}:{self.account}:rule/StepFunctionsGetEventsForEMRServerlessJobRule"
            ],
            actions=[
                "events:PutTargets",
                "events:PutRule",
                "events:DescribeRule"
            ]
        ))

        lmb_chk_upd.grant_invoke(sm)
        lmb_cmt_etl.grant_invoke(sm)
        

        # ========== EventBridge - schedule the ETL process 

        inpt = events.RuleTargetInput.from_object({
            "scripts_path": data_bucket.s3_url_for_object("etl-scripts"),
            "dwh_path": data_bucket.s3_url_for_object("data/dwh"),
            "emr_role_arn": emrRole.role_arn,
            "chk_upd_arn": lmb_chk_upd.function_arn,
            "cmt_etl_arn": lmb_cmt_etl.function_arn
        })

        trgt = event_targets.SfnStateMachine(machine=sm, input=inpt)

        events.Rule(
            self, 
            "RunPeriodicallySalesETL", 
            schedule=events.Schedule.cron(minute="*/30"), # every 30th minute for testing purposes
            targets=[
                trgt
            ]) #type: ignore