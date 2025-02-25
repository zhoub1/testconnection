#!/usr/bin/env python3
import aws_cdk as cdk
from lib.cdk_datazone_stack import CdkDatazoneStack

# REPLACE ME WITH UNIQUE STACK NAME IF INVOKING MORE THAN ONCE
unique_stack_name = f"datameshstackfrompipeline"

app = cdk.App()

# Retrieve account and region from CDK context
account = app.node.try_get_context('CDK_DEFAULT_ACCOUNT')
region = app.node.try_get_context('CDK_DEFAULT_REGION')

CdkDatazoneStack(
    app,
    unique_stack_name,
    env=cdk.Environment(account=account, region=region)
)

app.synth()
