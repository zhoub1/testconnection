from aws_cdk import (
    Stack,
    aws_s3 as s3,
    aws_s3_deployment as s3_deployment,
    aws_iam as iam,
    aws_glue as glue,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_events as events,
    aws_events_targets as targets,
    aws_logs as logs,
    RemovalPolicy,
    Duration,
    CfnOutput,
    Tags,
)
from constructs import Construct
import re


class DataMeshPipelineStack(Stack):
    """
    CDK Stack to set up an AWS Glue-based ETL pipeline with S3, Step Functions, and EventBridge integration.
    This version creates dedicated buckets for input, output, and the Glue script.
    The Glue script is deployed by explicitly referencing the file from your local directory.
    """

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Fetch parameters from context or use defaults
        glue_input_bucket_name = self.node.try_get_context("glue_input_bucket_name") or "default-input-bucket"
        glue_output_bucket_name = self.node.try_get_context("glue_output_bucket_name") or "default-output-bucket"
        glue_script_bucket_name = self.node.try_get_context("glue_script_bucket_name") or "default-script-bucket"
        glue_script_key = self.node.try_get_context("glue_script_key") or "default-script-key.py"
        glue_job_name = self.node.try_get_context("glue_job_name") or "default-glue-job"
        data_zone_source_bucket_name = self.node.try_get_context("data_zone_source_bucket_name")
        project_name = self.node.try_get_context("project_name") or "data-mesh-pipeline"
        environment = self.node.try_get_context("environment") or "production"

        # Validate input parameters
        self.validate_parameters(
            glue_input_bucket_name,
            glue_output_bucket_name,
            glue_script_bucket_name,
            glue_script_key,
            glue_job_name,
            data_zone_source_bucket_name,
        )

        # Apply tags
        Tags.of(self).add("Project", project_name)
        Tags.of(self).add("Environment", environment)

        # Sanitize bucket names
        glue_input_bucket_name = self.sanitize_bucket_name(glue_input_bucket_name)
        glue_output_bucket_name = self.sanitize_bucket_name(glue_output_bucket_name)
        glue_script_bucket_name = self.sanitize_bucket_name(glue_script_bucket_name)

        # Create S3 buckets
        glue_input_bucket = s3.Bucket(
            self,
            'GlueInputBucket',
            bucket_name=glue_input_bucket_name,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            event_bridge_enabled=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
        )

        if data_zone_source_bucket_name:
            glue_output_bucket = s3.Bucket.from_bucket_name(
                self, 'DataZoneSourceBucket', data_zone_source_bucket_name
            )
            output_bucket_name = data_zone_source_bucket_name
            output_bucket_arn = f"arn:aws:s3:::{data_zone_source_bucket_name}"
        else:
            glue_output_bucket = s3.Bucket(
                self,
                'GlueOutputBucket',
                bucket_name=glue_output_bucket_name,
                versioned=True,
                removal_policy=RemovalPolicy.DESTROY,
                auto_delete_objects=True,
                encryption=s3.BucketEncryption.S3_MANAGED,
            )
            output_bucket_name = glue_output_bucket.bucket_name
            output_bucket_arn = glue_output_bucket.bucket_arn

        glue_script_bucket = s3.Bucket(
            self,
            'GlueScriptBucket',
            bucket_name=glue_script_bucket_name,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
        )

        # IAM Role for the Glue Job
        glue_job_role = iam.Role(
            self,
            f"GlueJobRole-{re.sub(r'[^a-zA-Z0-9]', '', (project_name[:6] + environment[:4]))}",
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole')
            ],
        )

        # Deploy the Glue script by specifying the exact file path.
        # For example, if glue_script_key is "glueautomationscript.py", the file must reside at "scripts/glueautomationscript.py"
        glue_script_deployment = s3_deployment.BucketDeployment(
            self,
            'DeployGlueScript',
            sources=[s3_deployment.Source.asset(f'scripts/{glue_script_key}')],
            destination_bucket=glue_script_bucket,
            destination_key_prefix='',
            retain_on_delete=False,
        )

        # Grant permissions to the Glue job role
        glue_job_role.add_to_policy(
            iam.PolicyStatement(
                resources=[
                    glue_input_bucket.bucket_arn,
                    f"{glue_input_bucket.bucket_arn}/*",
                    output_bucket_arn,
                    f"{output_bucket_arn}/*",
                    glue_script_bucket.bucket_arn,
                    f"{glue_script_bucket.bucket_arn}/*",
                    f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws-glue/jobs/*",
                ],
                actions=[
                    's3:GetObject',
                    's3:PutObject',
                    's3:ListBucket',
                    'logs:CreateLogGroup',
                    'logs:CreateLogStream',
                    'logs:PutLogEvents',
                ],
            )
        )

        # Define the Glue job; script_location references the file in the dedicated script bucket.
        glue_job = glue.CfnJob(
            self,
            'GlueJob',
            name=glue_job_name,
            role=glue_job_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name='glueetl',
                script_location=f"s3://{glue_script_bucket.bucket_name}/{glue_script_key}",
                python_version='3',
            ),
            default_arguments={
                '--input_path': f"s3://{glue_input_bucket.bucket_name}/",
                '--output_path': f"s3://{output_bucket_name}/transformed/",
                '--enable-continuous-cloudwatch-log': 'true',
                '--enable-metrics': 'true',
                '--enable-glue-datacatalog': 'true',
                '--additional-python-modules': 'pandas,openpyxl',
            },
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=10
            ),
            max_retries=1,
            glue_version='4.0',
            timeout=2880,
            worker_type='G.4X',
            number_of_workers=10
        )

        # Ensure the Glue job is created only after the script is deployed.
        glue_job.node.add_dependency(glue_script_deployment)

        glue_job_arn = f"arn:aws:glue:{self.region}:{self.account}:job/{glue_job.name}"

        # IAM Role for Step Functions
        step_function_role = iam.Role(
            self,
            f"StepFunctionRole-{re.sub(r'[^a-zA-Z0-9]', '', (project_name[:6] + environment[:4]))}",
            assumed_by=iam.ServicePrincipal('states.amazonaws.com'),
        )

        step_function_role.add_to_policy(
            iam.PolicyStatement(
                resources=[glue_job_arn],
                actions=[
                    'glue:StartJobRun',
                    'glue:GetJobRun',
                    'glue:GetJobRuns',
                    'glue:BatchGetJobs',
                    'glue:GetJob',
                ],
            )
        )

        step_function_role.add_to_policy(
            iam.PolicyStatement(
                resources=[
                    f"arn:aws:logs:{self.region}:{self.account}:log-group:/aws/vendedlogs/states/*"
                ],
                actions=[
                    'logs:CreateLogDelivery',
                    'logs:GetLogDelivery',
                    'logs:UpdateLogDelivery',
                    'logs:DeleteLogDelivery',
                    'logs:ListLogDeliveries',
                    'logs:PutResourcePolicy',
                    'logs:DescribeResourcePolicies',
                    'logs:DescribeLogGroups',
                ],
            )
        )

        # Create a unique log group for the state machine.
        unique_log_group_name = f"/aws/vendedlogs/states/{project_name}-{environment}-{self.account}-{self.region}-state-machine"
        log_group = logs.LogGroup(
            self,
            f"StateMachineLogGroup-{re.sub(r'[^a-zA-Z0-9]', '', unique_log_group_name)}",
            log_group_name=unique_log_group_name,
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY,
        )

        glue_job_task = tasks.GlueStartJobRun(
            self,
            'StartGlueJob',
            glue_job_name=glue_job.name,
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            arguments=sfn.TaskInput.from_object({
                '--input_path': f"s3://{glue_input_bucket.bucket_name}/",
                '--object_key': sfn.JsonPath.string_at('$.detail.object.key'),
                '--output_path': f"s3://{output_bucket_name}/transformed/",
            }),
            result_path='$.glueResult',
            task_timeout=sfn.Timeout.duration(Duration.hours(2)),
        )

        wait_before_starting_job = sfn.Wait(
            self,
            'WaitBeforeStartingJob',
            time=sfn.WaitTime.duration(Duration.seconds(10)),
        )

        state_machine = sfn.StateMachine(
            self,
            f"GlueStateMachine-{re.sub(r'[^a-zA-Z0-9]', '', (project_name[:6] + environment[:4]))}",
            definition_body=sfn.DefinitionBody.from_chainable(
                wait_before_starting_job.next(glue_job_task)
            ),
            role=step_function_role,
            timeout=Duration.hours(3),
            logs=sfn.LogOptions(
                destination=log_group,
                level=sfn.LogLevel.ALL,
                include_execution_data=True,
            ),
            tracing_enabled=True,
        )

        rule = events.Rule(
            self,
            f"S3EventRule-{re.sub(r'[^a-zA-Z0-9]', '', (project_name[:6] + environment[:4]))}",
            event_pattern=events.EventPattern(
                source=['aws.s3'],
                detail_type=['Object Created'],
                detail={
                    'bucket': {'name': [glue_input_bucket.bucket_name]},
                    'object': {'key': [{'prefix': ''}]}
                },
            ),
        )
        rule.add_target(targets.SfnStateMachine(state_machine))

        CfnOutput(self, 'GlueInputBucketName', value=glue_input_bucket.bucket_name)
        if not data_zone_source_bucket_name:
            CfnOutput(self, 'GlueOutputBucketName', value=glue_output_bucket.bucket_name)
        CfnOutput(self, 'GlueScriptBucketName', value=glue_script_bucket.bucket_name)
        CfnOutput(self, 'GlueJobName', value=glue_job.name)
        CfnOutput(self, 'StateMachineArn', value=state_machine.state_machine_arn)

    def validate_parameters(
        self,
        glue_input_bucket_name: str,
        glue_output_bucket_name: str,
        glue_script_bucket_name: str,
        glue_script_key: str,
        glue_job_name: str,
        data_zone_source_bucket_name: str,
    ) -> None:
        errors = []
        bucket_name_pattern = r'^[a-z0-9.-]{3,63}$'
        for name, param in [
            ('glue_input_bucket_name', glue_input_bucket_name),
            ('glue_output_bucket_name', glue_output_bucket_name),
            ('glue_script_bucket_name', glue_script_bucket_name),
        ]:
            if not re.match(bucket_name_pattern, param):
                errors.append(
                    f"Invalid bucket name '{param}' for '{name}'. Bucket names must be between 3 and 63 characters and can contain lowercase letters, numbers, periods, and hyphens."
                )
        job_name_pattern = r'^[a-zA-Z0-9-_]{1,255}$'
        if not re.match(job_name_pattern, glue_job_name):
            errors.append(
                f"Invalid Glue job name '{glue_job_name}'. Job names can contain letters, numbers, hyphens, and underscores, and must be between 1 and 255 characters."
            )
        if not glue_script_key:
            errors.append("Glue script key cannot be empty.")
        if errors:
            error_message = "\n".join(errors)
            self.node.add_error(f"Parameter validation failed:\n{error_message}")

    @staticmethod
    def sanitize_bucket_name(name: str) -> str:
        name = name.lower()
        name = re.sub(r'[^a-z0-9.-]', '-', name)
        name = re.sub(r'\.\.+', '.', name)
        name = name.strip('.-')
        if len(name) < 3 or len(name) > 63:
            raise ValueError(f"S3 bucket name '{name}' must be between 3 and 63 characters long.")
        return name
