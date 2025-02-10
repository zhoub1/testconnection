import aws_cdk as cdk
from aws_cdk import aws_s3 as s3

class SimplePipelineStack(cdk.Stack):
    def __init__(self, scope: cdk.App, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # Creating a unique S3 bucket
        bucket = s3.Bucket(
            self, 
            "PIPELINETESTNAMECHANGE021025",
            versioned=True,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # Output the bucket name
        cdk.CfnOutput(self, "BucketName", value=bucket.bucket_name)
