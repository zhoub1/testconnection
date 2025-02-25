#!/usr/bin/env python3
import aws_cdk as cdk
from lib.data_mesh_pipeline_stack import DataMeshPipelineStack

# REPLACE ME WITH UNIQUE STACK NAME IF INVOKING MORE THAN ONCE
unique_stack_name = f"CDKDataMeshPipel022425"

app = cdk.App()

# Retrieve account and region from CDK context
account = app.node.try_get_context('CDK_DEFAULT_ACCOUNT')
region = app.node.try_get_context('CDK_DEFAULT_REGION')

# Instantiate the DataMeshPipelineStack with environment settings
DataMeshPipelineStack(app,
                      unique_stack_name,
                      env=cdk.Environment(account=account, region=region)
                      )

# Synthesize the CloudFormation template
app.synth()
