from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,

    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_iam as iam,
    aws_glue as glue,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
)

from constructs import Construct


class TallerDatawarehouseAwsIaCStack(Stack):

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs
    ) -> None:

        super().__init__(
            scope,
            construct_id,
            **kwargs
        )

        # ============================================================
        # 1. RAW / BRONZE BUCKET
        # ============================================================

        self.raw_bucket = s3.Bucket(
            self,
            "RawDataBucket",

            versioned=True,

            encryption=s3.BucketEncryption.S3_MANAGED,

            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,

            removal_policy=RemovalPolicy.DESTROY,

            auto_delete_objects=True,
        )


        # ============================================================
        # 2. SILVER / TRUSTED BUCKET
        # ============================================================

        self.silver_bucket = s3.Bucket(
            self,
            "SilverDataBucket",

            versioned=True,

            encryption=s3.BucketEncryption.S3_MANAGED,

            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,

            removal_policy=RemovalPolicy.DESTROY,

            auto_delete_objects=True,
        )


        # ============================================================
        # 3. GOLD BUCKET
        # ============================================================

        self.gold_bucket = s3.Bucket(
            self,
            "GoldDataBucket",

            versioned=True,

            encryption=s3.BucketEncryption.S3_MANAGED,

            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,

            removal_policy=RemovalPolicy.DESTROY,

            auto_delete_objects=True,
        )


        # ============================================================
        # 4. SCRIPTS BUCKET
        # ============================================================

        self.scripts_bucket = s3.Bucket(
            self,
            "ScriptsBucket",

            versioned=True,

            encryption=s3.BucketEncryption.S3_MANAGED,

            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,

            removal_policy=RemovalPolicy.DESTROY,

            auto_delete_objects=True,
        )


        # ============================================================
        # 5. DEPLOY GLUE SCRIPTS
        # ============================================================

        self.glue_scripts = s3deploy.BucketDeployment(
            self,
            "DeployGlueScripts",

            sources=[
                s3deploy.Source.asset("glue")
            ],

            destination_bucket=self.scripts_bucket,

            destination_key_prefix="glue",
        )


        # ============================================================
        # 6. GLUE IAM ROLE
        # ============================================================

        self.glue_role = iam.Role(
            self,
            "GlueServiceRole",

            assumed_by=iam.ServicePrincipal(
                "glue.amazonaws.com"
            ),

            description=(
                "IAM Role used by AWS Glue "
                "for the Data Warehouse ETL pipeline"
            ),
        )


        # ============================================================
        # 7. GLUE MANAGED POLICY
        # ============================================================

        self.glue_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSGlueServiceRole"
            )
        )


        # ============================================================
        # 8. S3 PERMISSIONS FOR GLUE
        # ============================================================

        self.glue_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,

                actions=[
                    "s3:GetObject",
                    "s3:GetObjectVersion",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                ],

                resources=[
                    self.raw_bucket.bucket_arn,
                    f"{self.raw_bucket.bucket_arn}/*",

                    self.silver_bucket.bucket_arn,
                    f"{self.silver_bucket.bucket_arn}/*",

                    self.gold_bucket.bucket_arn,
                    f"{self.gold_bucket.bucket_arn}/*",

                    self.scripts_bucket.bucket_arn,
                    f"{self.scripts_bucket.bucket_arn}/*",
                ],
            )
        )


        # ============================================================
        # 9. GLUE DATABASE
        # ============================================================

        self.glue_database = glue.CfnDatabase(
            self,
            "DataWarehouseGlueDatabase",

            catalog_id=self.account,

            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="datawarehouse"
            ),
        )


        # ============================================================
        # 10. BRONZE → SILVER GLUE JOB
        # ============================================================

        self.bronze_to_silver_job = glue.CfnJob(
            self,
            "BronzeToSilverJob",

            name="datawarehouse-bronze-to-silver",

            role=self.glue_role.role_arn,

            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",

                python_version="3",

                script_location=(
                    f"s3://"
                    f"{self.scripts_bucket.bucket_name}"
                    "/glue/bronze_to_silver.py"
                ),
            ),

            glue_version="5.0",

            number_of_workers=2,

            worker_type="G.1X",

            execution_property=(
                glue.CfnJob.ExecutionPropertyProperty(
                    max_concurrent_runs=1
                )
            ),

            default_arguments={

                "--job-language": "python",

                "--enable-metrics": "",

                "--enable-continuous-cloudwatch-log": "true",

                "--enable-spark-ui": "true",

                "--enable-job-insights": "true",

                "--S3_RAW_PATH": (
                    f"s3://"
                    f"{self.raw_bucket.bucket_name}"
                    "/sales.csv"
                ),

                "--S3_SILVER_PATH": (
                    f"s3://"
                    f"{self.silver_bucket.bucket_name}"
                    "/sales/"
                ),
            },
        )


        # ============================================================
        # 11. SILVER → GOLD GLUE JOB
        # ============================================================

        self.silver_to_gold_job = glue.CfnJob(
            self,
            "SilverToGoldJob",

            name="datawarehouse-silver-to-gold",

            role=self.glue_role.role_arn,

            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",

                python_version="3",

                script_location=(
                    f"s3://"
                    f"{self.scripts_bucket.bucket_name}"
                    "/glue/silver_to_gold.py"
                ),
            ),

            glue_version="5.0",

            number_of_workers=2,

            worker_type="G.1X",

            execution_property=(
                glue.CfnJob.ExecutionPropertyProperty(
                    max_concurrent_runs=1
                )
            ),

            default_arguments={

                "--job-language": "python",

                "--enable-metrics": "",

                "--enable-continuous-cloudwatch-log": "true",

                "--enable-spark-ui": "true",

                "--enable-job-insights": "true",

                "--S3_SILVER_PATH": (
                    f"s3://"
                    f"{self.silver_bucket.bucket_name}"
                    "/sales/"
                ),

                "--S3_GOLD_PATH": (
                    f"s3://"
                    f"{self.gold_bucket.bucket_name}"
                ),
            },
        )


        # ============================================================
        # 12. DEPENDENCY:
        #
        # Scripts must be uploaded before Glue Jobs are created.
        # ============================================================

        self.bronze_to_silver_job.node.add_dependency(
            self.glue_scripts
        )

        self.silver_to_gold_job.node.add_dependency(
            self.glue_scripts
        )


        # ============================================================
        # 13. STEP FUNCTIONS - BRONZE → SILVER
        # ============================================================
        #
        # Step Functions ejecutará el Glue Job y esperará
        # hasta que termine.
        #
        # RUN_JOB = Start Job + esperar a que termine.
        #
        # ============================================================

        bronze_to_silver_task = tasks.GlueStartJobRun(
            self,
            "BronzeToSilverTask",

            glue_job_name=(
                self.bronze_to_silver_job.ref
            ),

            integration_pattern=(
                sfn.IntegrationPattern.RUN_JOB
            ),

            arguments=sfn.TaskInput.from_object({

                "--S3_RAW_PATH": (
                    f"s3://"
                    f"{self.raw_bucket.bucket_name}"
                    "/sales.csv"
                ),

                "--S3_SILVER_PATH": (
                    f"s3://"
                    f"{self.silver_bucket.bucket_name}"
                    "/sales/"
                ),
            }),

            timeout=Duration.minutes(30),
        )


        # ============================================================
        # 14. STEP FUNCTIONS - SILVER → GOLD
        # ============================================================

        silver_to_gold_task = tasks.GlueStartJobRun(
            self,
            "SilverToGoldTask",

            glue_job_name=(
                self.silver_to_gold_job.ref
            ),

            integration_pattern=(
                sfn.IntegrationPattern.RUN_JOB
            ),

            arguments=sfn.TaskInput.from_object({

                "--S3_SILVER_PATH": (
                    f"s3://"
                    f"{self.silver_bucket.bucket_name}"
                    "/sales/"
                ),

                "--S3_GOLD_PATH": (
                    f"s3://"
                    f"{self.gold_bucket.bucket_name}"
                ),
            }),

            timeout=Duration.minutes(30),
        )


        # ============================================================
        # 15. STEP FUNCTIONS WORKFLOW
        # ============================================================
        #
        # BRONZE → SILVER
        #       ↓
        # SILVER → GOLD
        #
        # El segundo Job NO comienza hasta que el primero
        # termine correctamente.
        #
        # ============================================================

        workflow = (
            bronze_to_silver_task
            .next(silver_to_gold_task)
        )


        # ============================================================
        # 16. STATE MACHINE
        # ============================================================

        self.state_machine = sfn.StateMachine(
            self,
            "DataWarehouseStateMachine",

            state_machine_name=(
                "datawarehouse-taller-pipeline"
            ),

            definition_body=sfn.DefinitionBody.from_chainable(
                workflow
            ),

            timeout=Duration.minutes(60),

            tracing_enabled=True,
        )


        # ============================================================
        # 17. STEP FUNCTIONS → GLUE PERMISSIONS
        # ============================================================
        #
        # Permitimos que Step Functions ejecute los Jobs.
        #
        # ============================================================

        self.state_machine.role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,

                actions=[
                    "glue:StartJobRun",
                    "glue:GetJobRun",
                    "glue:GetJobRuns",
                    "glue:BatchStopJobRun",
                ],

                resources=[
                    self.format_arn(
                        service="glue",
                        resource="job",
                        resource_name=self.bronze_to_silver_job.ref,
                    ),
                    self.format_arn(
                        service="glue",
                        resource="job",
                        resource_name=self.silver_to_gold_job.ref,
                    ),
                ],
            )
        )