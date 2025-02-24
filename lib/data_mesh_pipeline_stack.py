import hashlib
import re
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

# Embedded Glue script (streamlined and cleaned up)
EMBEDDED_GLUE_SCRIPT = r'''import sys, logging, pandas as pd
from datetime import datetime
from dateutil import parser
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import trim, col, udf, when, regexp_replace, isnan
from pyspark.sql.types import *
from pyspark.sql import DataFrame

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger()

def init_glue():
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'object_key', 'output_path'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    return glueContext, glueContext.spark_session, job, args

def detect_delim(spark, path, sample_size=10):
    sc = spark.sparkContext
    lines = (sc.textFile(path, minPartitions=1)
               .zipWithIndex()
               .filter(lambda x: x[1] < sample_size)
               .map(lambda x: x[0])
               .collect())
    possibles = [',', '\t', ';', '|']
    counts = {d: sum(line.count(d) for line in lines) for d in possibles}
    delim = max(counts, key=counts.get)
    logger.info(f"Detected delimiter: {delim}")
    return delim

def read_csv(spark, path):
    delim = detect_delim(spark, path)
    return (spark.read.options(header="true", inferSchema="true", quote='"',
                               escape='"', multiLine="true", delimiter=delim)
                  .csv(path))

def read_input(spark, base, key):
    full_path = f"{base.rstrip('/')}/{key.lstrip('/')}"
    ext = full_path.split('.')[-1].lower()
    if ext == 'csv':
        return read_csv(spark, full_path), ext
    elif ext == 'txt':
        return spark.read.text(full_path), ext
    elif ext == 'xlsx':
        df = pd.read_excel(full_path, engine='openpyxl')
        return spark.createDataFrame(df), ext
    elif ext == 'json':
        return spark.read.options(multiLine="true").json(full_path), ext
    elif ext == 'parquet':
        return spark.read.parquet(full_path), ext
    elif ext == 'xml':
        return (spark.read.format("com.databricks.spark.xml")
                      .option("rowTag", "row")
                      .load(full_path), ext)
    else:
        raise ValueError("Unsupported file format")

def standardize_dates(col_obj, fmts=None):
    fmts = fmts or ['%Y-%m-%d','%m/%d/%Y','%d/%m/%Y','%Y/%m/%d',
                    '%Y.%m.%d','%d-%b-%Y','%b %d, %Y','%Y%m%d']
    def parse_date(s):
        if not s: 
            return None
        for fmt in fmts:
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                continue
        try:
            return parser.parse(s)
        except Exception:
            return None
    return udf(parse_date, TimestampType())(col_obj)

def transform(df: DataFrame) -> DataFrame:
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(col(field.name)))
    date_cols = [f.name for f in df.schema.fields if 'date' in f.name.lower()]
    for d in date_cols:
        df = df.withColumn(d, standardize_dates(col(d)))
    num_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType) and 'id' not in f.name.lower()]
    for n in num_cols:
        df = df.withColumn(n, when(col(n).rlike('^-?\\d+(\\.\\d+)?$'),
                                     col(n).cast(DoubleType())).otherwise(col(n)))
        df = df.withColumn(n, when(col(n).rlike('^-?\\d+(\\.\\d+)?%$'),
                                     (regexp_replace(col(n), '%', '').cast(DoubleType())/100))
                                     .otherwise(col(n)))
    return df

def write_output(glueContext, df, output_base, key, file_type):
    ts = datetime.utcnow().strftime('%Y-%m-%d')
    base_name = key.split('/')[-1].split('.')[0]
    out_path = f"{output_base.rstrip('/')}/{base_name}_{file_type}_processed_{ts}/"
    count = df.count()
    if count <= 1000000:
        df = df.coalesce(1)
    dynf = DynamicFrame.fromDF(df, glueContext, "dynf")
    glueContext.write_dynamic_frame.from_options(
        frame=dynf,
        connection_type="s3",
        connection_options={"path": out_path},
        format="parquet"
    )
    logger.info(f"Data written to {out_path}")

def main():
    try:
        glueContext, spark, job, args = init_glue()
        df, ext = read_input(spark, args['input_path'], args['object_key'])
        df = transform(df)
        write_output(glueContext, df, args['output_path'], args['object_key'], ext)
        logger.info("Job completed successfully.")
    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise e
    finally:
        job.commit()

if __name__ == '__main__':
    main()'''

class DataMeshPipelineStack(Stack):
    """
    CDK Stack to set up an AWS Glue-based ETL pipeline with S3, Step Functions, and EventBridge integration.
    """
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Fetch parameters from context or use defaults
        glue_input_bucket_name = self.node.try_get_context("glue_input_bucket_name") or "default-input-bucket"
        glue_output_bucket_name = self.node.try_get_context("glue_output_bucket_name") or "default-output-bucket"
        glue_script_bucket_name = self.node.try_get_context("glue_script_bucket_name") or "default-script-bucket"
        glue_script_key = self.node.try_get_context("glue_script_key") or "glue_script.py"
        glue_job_name = self.node.try_get_context("glue_job_name") or "default-glue-job"
        data_zone_source_bucket_name = self.node.try_get_context("data_zone_source_bucket_name")
        project_name = self.node.try_get_context("project_name") or "data-mesh-pipeline"
        environment = self.node.try_get_context("environment") or "development"

        # Validate input parameters
        self.validate_parameters(
            glue_input_bucket_name,
            glue_output_bucket_name,
            glue_script_bucket_name,
            glue_script_key,
            glue_job_name,
            data_zone_source_bucket_name,
        )

        # Apply tags to the Stack
        Tags.of(self).add("Project", project_name)
        Tags.of(self).add("Environment", environment)

        # Sanitize bucket names
        glue_input_bucket_name = self.sanitize_bucket_name(glue_input_bucket_name)
        glue_output_bucket_name = self.sanitize_bucket_name(glue_output_bucket_name)
        glue_script_bucket_name = self.sanitize_bucket_name(glue_script_bucket_name)

        # Create S3 Input Bucket for incoming data
        glue_input_bucket = s3.Bucket(
            self,
            'GlueInputBucket',
            bucket_name=glue_input_bucket_name,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            event_bridge_enabled=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
        )

        # Create or use existing S3 Output Bucket for transformed data
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

        # Create S3 Script Bucket for Glue scripts
        glue_script_bucket = s3.Bucket(
            self,
            'GlueScriptBucket',
            bucket_name=glue_script_bucket_name,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
        )

        # IAM Role for the AWS Glue Job
        glue_job_role = iam.Role(
            self,
            f"GlueJobRole-{re.sub(r'[^a-zA-Z0-9]', '', (project_name[:6] + environment[:4]))}",
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole')
            ],
        )

        # Deploy the embedded Glue script to the script bucket
        s3_deployment.BucketDeployment(
            self,
            'DeployGlueScript',
            sources=[s3_deployment.Source.data(glue_script_key, EMBEDDED_GLUE_SCRIPT)],
            destination_bucket=glue_script_bucket,
            destination_key_prefix='',
            retain_on_delete=False,
        )

        # Add permissions for the Glue job role
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

        # Define the AWS Glue Job with increased capacity
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
            timeout=2880,  # 48 hours
            worker_type='G.4X',
            number_of_workers=10
        )

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

        # Ninja-level unique log group name:
        # Generate a unique 4-character suffix using a SHA1 hash of the node's ID.
        unique_suffix = hashlib.sha1(self.node.id.encode("utf-8")).hexdigest()[:4]
        log_group = logs.LogGroup(
            self,
            f"StateMachineLogGroup-{re.sub(r'[^a-zA-Z0-9]', '', (project_name[:8] + environment[:6]))}-{unique_suffix}",
            log_group_name=f"/aws/vendedlogs/states/{project_name}-{environment}-state-machine-{unique_suffix}",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Step Function Task to trigger the Glue Job
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

        # Step Function workflow with a wait before starting the Glue Job
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

        # EventBridge Rule to trigger the state machine on new S3 object creation
        rule = events.Rule(
            self,
            f"S3EventRule-{re.sub(r'[^a-zA-Z0-9]', '', (project_name[:6] + environment[:4]))}",
            event_pattern=events.EventPattern(
                source=['aws.s3'],
                detail_type=['Object Created'],
                detail={
                    'bucket': {
                        'name': [glue_input_bucket.bucket_name]
                    },
                    'object': {
                        'key': [{'prefix': ''}]
                    }
                },
            ),
        )
        rule.add_target(targets.SfnStateMachine(state_machine))

        # CloudFormation Outputs
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
                    f"Invalid bucket name '{param}' for '{name}'. "
                    "Bucket names must be 3-63 characters using lowercase letters, numbers, periods, and hyphens."
                )
        job_name_pattern = r'^[a-zA-Z0-9-_]{1,255}$'
        if not re.match(job_name_pattern, glue_job_name):
            errors.append(
                f"Invalid Glue job name '{glue_job_name}'. "
                "Job names must be 1-255 characters with letters, numbers, hyphens, and underscores."
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
