"""
Test suite for the DataMeshPipelineStack.

Authors:
- Bill Zhou
- Justin Miles

This test suite validates the AWS CDK stack that provisions an AWS Glue-based ETL pipeline with S3, Step Functions, and EventBridge integration.

The tests cover:
- S3 bucket configurations
- IAM roles and policies
- AWS Glue job properties
- AWS Step Functions state machine
- EventBridge rule configurations
- CloudFormation outputs
- Logging configurations
- Resource tagging
- Sanitation of S3 bucket names
- Removal policies and encryption settings

Each test provides informative assertion messages for better test results output.
"""

import aws_cdk as cdk
from aws_cdk import assertions
from lib.data_mesh_pipeline_stack import DataMeshPipelineStack
import re

def test_s3_buckets():
    """Test that the S3 buckets are created with correct properties."""
    app = cdk.App()
    stack = DataMeshPipelineStack(app, "DataMeshPipelineStack")
    template = assertions.Template.from_stack(stack)

    # Check that three S3 buckets are created
    template.resource_count_is("AWS::S3::Bucket", 3)

    # Check that all buckets have encryption enabled
    template.has_resource_properties(
        "AWS::S3::Bucket",
        {
            "BucketEncryption": assertions.Match.object_like({
                "ServerSideEncryptionConfiguration": [
                    {
                        "ServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "AES256"
                        }
                    }
                ]
            })
        }
    )

    # Ensure at least one bucket is versioned (the output bucket)
    template.has_resource_properties(
        "AWS::S3::Bucket",
        {
            "VersioningConfiguration": {
                "Status": "Enabled"
            }
        }
    )

    # Check that buckets have removal policies set to DESTROY
    resources = template.find_resources("AWS::S3::Bucket")
    for resource in resources.values():
        assert resource.get("DeletionPolicy") == "Delete", "S3 Bucket does not have DeletionPolicy set to Delete"
        assert resource.get("UpdateReplacePolicy") == "Delete", "S3 Bucket does not have UpdateReplacePolicy set to Delete"

def test_iam_roles():
    """Test that IAM roles for Glue and Step Functions are created with correct assume role policies."""
    app = cdk.App()
    stack = DataMeshPipelineStack(app, "DataMeshPipelineStack")
    template = assertions.Template.from_stack(stack)

    # Check for IAM Role for Glue job
    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "AssumeRolePolicyDocument": assertions.Match.object_like({
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "glue.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            })
        }
    )

    # Check for IAM Role for Step Functions
    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "AssumeRolePolicyDocument": assertions.Match.object_like({
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "states.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            })
        }
    )

def test_glue_job():
    """Test that the Glue Job is created with correct configurations."""
    app = cdk.App()
    stack = DataMeshPipelineStack(app, "DataMeshPipelineStack")
    template = assertions.Template.from_stack(stack)

    # Check for Glue Job with specific configurations
    template.has_resource_properties(
        "AWS::Glue::Job",
        {
            "Command": {
                "Name": "glueetl",
                "PythonVersion": "3"
            },
            "DefaultArguments": assertions.Match.object_like({
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics": "true",
                "--enable-glue-datacatalog": "true",
                "--additional-python-modules": "pandas,openpyxl"
            }),
            "ExecutionProperty": {
                "MaxConcurrentRuns": 10
            },
            "GlueVersion": "4.0",
            "WorkerType": "G.4X",
            "NumberOfWorkers": 10,
            "Timeout": 2880  # 48 hours in minutes
        }
    )

def test_state_machine():
    """Test that the Step Functions state machine is created with correct logging and tracing configurations."""
    app = cdk.App()
    stack = DataMeshPipelineStack(app, "DataMeshPipelineStack")
    template = assertions.Template.from_stack(stack)

    # Verify State Machine existence and its logging configuration
    template.resource_count_is("AWS::StepFunctions::StateMachine", 1)
    template.has_resource_properties(
        "AWS::StepFunctions::StateMachine",
        {
            "LoggingConfiguration": assertions.Match.object_like({
                "Level": "ALL",
                "IncludeExecutionData": True
            }),
            "TracingConfiguration": {
                "Enabled": True
            },
            "RoleArn": assertions.Match.any_value(),
            "DefinitionString": assertions.Match.any_value()
        }
    )

def test_eventbridge_rule():
    """Test that the EventBridge rule is configured to trigger on S3 object creation."""
    app = cdk.App()
    stack = DataMeshPipelineStack(app, 'DataMeshPipelineStack')
    template = assertions.Template.from_stack(stack)

    # Check for an EventBridge Rule triggering the state machine on S3 object creation
    template.resource_count_is("AWS::Events::Rule", 1)
    template.has_resource_properties(
        "AWS::Events::Rule",
        {
            "EventPattern": assertions.Match.object_like({
                "source": ["aws.s3"],
                "detail-type": ["Object Created"]
            })
        }
    )

def test_outputs():
    """Test that the CloudFormation outputs are correctly defined."""
    app = cdk.App()
    stack = DataMeshPipelineStack(app, "DataMeshPipelineStack")
    template = assertions.Template.from_stack(stack)

    # Check that the outputs are created
    outputs = [
        "GlueInputBucketName",
        "GlueScriptBucketName",
        "GlueJobName",
        "StateMachineArn"
    ]
    for output in outputs:
        template.has_output(output, {})

def test_glue_job_role_policies():
    """Test that the IAM policies attached to the Glue job role are correct."""
    app = cdk.App()
    stack = DataMeshPipelineStack(app, "DataMeshPipelineStack")
    template = assertions.Template.from_stack(stack)

    # Check that the Glue job role has the required policies
    template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement": assertions.Match.array_with([
                    assertions.Match.object_like({
                        "Effect": "Allow",
                        "Action": assertions.Match.array_with([
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:ListBucket",
                            "logs:CreateLogGroup",
                            "logs:CreateLogStream",
                            "logs:PutLogEvents"
                        ]),
                        "Resource": assertions.Match.any_value()
                    })
                ])
            }
        }
    )

def test_step_function_role_policies():
    """Test that the IAM policies attached to the Step Functions role are correct."""
    app = cdk.App()
    stack = DataMeshPipelineStack(app, "DataMeshPipelineStack")
    template = assertions.Template.from_stack(stack)

    # Check that the Step Functions role has the required policies
    template.has_resource_properties(
        "AWS::IAM::Policy",
        {
            "PolicyDocument": {
                "Statement": assertions.Match.array_with([
                    assertions.Match.object_like({
                        "Effect": "Allow",
                        "Action": assertions.Match.array_with([
                            "glue:StartJobRun",
                            "glue:GetJobRun",
                            "glue:GetJobRuns",
                            "glue:BatchGetJobs",
                            "glue:GetJob"
                        ]),
                        "Resource": assertions.Match.any_value()
                    }),
                    assertions.Match.object_like({
                        "Effect": "Allow",
                        "Action": assertions.Match.array_with([
                            "logs:CreateLogDelivery",
                            "logs:GetLogDelivery",
                            "logs:UpdateLogDelivery",
                            "logs:DeleteLogDelivery",
                            "logs:ListLogDeliveries",
                            "logs:PutResourcePolicy",
                            "logs:DescribeResourcePolicies",
                            "logs:DescribeLogGroups"
                        ]),
                        "Resource": assertions.Match.any_value()
                    })
                ])
            }
        }
    )

def test_state_machine_tracing():
    """Test that X-Ray tracing is enabled for the state machine."""
    app = cdk.App()
    stack = DataMeshPipelineStack(app, "DataMeshPipelineStack")
    template = assertions.Template.from_stack(stack)

    # Verify that the State Machine has X-Ray tracing enabled
    template.has_resource_properties(
        "AWS::StepFunctions::StateMachine",
        {
            "TracingConfiguration": {
                "Enabled": True
            }
        }
    )

def test_glue_job_default_arguments():
    """Test that the Glue Job's default arguments are correctly set."""
    app = cdk.App()
    stack = DataMeshPipelineStack(app, "DataMeshPipelineStack")
    template = assertions.Template.from_stack(stack)

    # Check that the Glue Job's default arguments include expected keys
    template.has_resource_properties(
        "AWS::Glue::Job",
        {
            "DefaultArguments": assertions.Match.object_like({
                "--input_path": assertions.Match.any_value(),
                "--output_path": assertions.Match.any_value(),
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-metrics": "true",
                "--enable-glue-datacatalog": "true",
                "--additional-python-modules": "pandas,openpyxl"
            })
        }
    )

def test_state_machine_wait_state():
    """Test that the state machine includes a Wait state named 'WaitBeforeStartingJob'."""
    app = cdk.App()
    stack = DataMeshPipelineStack(app, 'DataMeshPipelineStack')
    template = assertions.Template.from_stack(stack)

    # Get the entire template as a dictionary
    template_dict = template.to_json()

    # Find the State Machine resource
    resources = template_dict.get('Resources', {})
    state_machine_resource = None
    for resource in resources.values():
        if resource['Type'] == 'AWS::StepFunctions::StateMachine':
            state_machine_resource = resource
            break

    assert state_machine_resource is not None, "State Machine resource not found in template"

    definition = state_machine_resource['Properties']['DefinitionString']

    # Resolve the DefinitionString
    resolved_definition = resolve_definition_string(definition)

    # Now parse the JSON
    import json
    try:
        definition_dict = json.loads(resolved_definition)
    except json.JSONDecodeError as e:
        assert False, f"Failed to parse State Machine definition: {e}"

    # Check for the Wait state
    assert 'WaitBeforeStartingJob' in definition_dict['States'], "WaitBeforeStartingJob state not found"
    assert definition_dict['States']['WaitBeforeStartingJob']['Type'] == 'Wait', "WaitBeforeStartingJob is not of Type 'Wait'"

def resolve_definition_string(definition):
    """Recursively resolve CloudFormation intrinsic functions in the state machine definition."""
    if isinstance(definition, str):
        return definition
    elif isinstance(definition, dict):
        if 'Fn::Join' in definition:
            delimiter = definition['Fn::Join'][0]
            parts = definition['Fn::Join'][1]
            resolved_parts = [resolve_definition_string(part) for part in parts]
            return delimiter.join(resolved_parts)
        elif 'Ref' in definition or 'Fn::GetAtt' in definition:
            # Return a placeholder for Ref and Fn::GetAtt
            return ''
        elif 'Fn::Sub' in definition:
            # Handle Fn::Sub
            sub_value = definition['Fn::Sub']
            if isinstance(sub_value, str):
                return sub_value
            elif isinstance(sub_value, list):
                return sub_value[0]
            else:
                return ''
        else:
            # Handle other intrinsic functions if necessary
            return ''
    elif isinstance(definition, list):
        resolved_parts = [resolve_definition_string(part) for part in definition]
        return ''.join(resolved_parts)
    else:
        # For other types, return empty string
        return ''

def test_log_group():
    """Test that the Log Group is created with a retention policy of one month."""
    app = cdk.App()
    stack = DataMeshPipelineStack(app, "DataMeshPipelineStack")
    template = assertions.Template.from_stack(stack)

    # Verify that a Log Group is created with a retention policy of one month
    template.has_resource_properties(
        "AWS::Logs::LogGroup",
        {
            "RetentionInDays": 30
        }
    )

def test_tags():
    """Test that the S3 buckets have the 'Project' tag applied."""
    app = cdk.App()
    stack = DataMeshPipelineStack(app, "DataMeshPipelineStack")
    template = assertions.Template.from_stack(stack)

    # Check that at least one S3 bucket has the 'Project' tag
    template.has_resource_properties(
        "AWS::S3::Bucket",
        {
            "Tags": assertions.Match.array_with([
                assertions.Match.object_like({
                    "Key": "Project",
                    "Value": assertions.Match.any_value()
                })
            ])
        }
    )

def test_sanitize_bucket_name():
    """Test that the sanitize_bucket_name function correctly sanitizes bucket names."""
    from lib.data_mesh_pipeline_stack import DataMeshPipelineStack
    sanitized_name = DataMeshPipelineStack.sanitize_bucket_name("Invalid_Bucket_Name")
    assert sanitized_name == "invalid-bucket-name", f"Bucket name not sanitized correctly: {sanitized_name}"

def test_removal_policies():
    """Test that resources have the correct removal policies."""
    app = cdk.App()
    stack = DataMeshPipelineStack(app, 'DataMeshPipelineStack')
    template = assertions.Template.from_stack(stack)

    # Check that S3 buckets have DeletionPolicy set to Delete
    resources = template.find_resources("AWS::S3::Bucket")
    for resource in resources.values():
        assert resource.get("DeletionPolicy") == "Delete", "S3 Bucket does not have DeletionPolicy set to Delete"
        assert resource.get("UpdateReplacePolicy") == "Delete", "S3 Bucket does not have UpdateReplacePolicy set to Delete"

def test_encryption_settings():
    """Test that all S3 buckets have encryption enabled."""
    app = cdk.App()
    stack = DataMeshPipelineStack(app, 'DataMeshPipelineStack')
    template = assertions.Template.from_stack(stack)

    # Check that all S3 buckets have encryption enabled
    template.has_resource_properties(
        "AWS::S3::Bucket",
        {
            "BucketEncryption": assertions.Match.any_value()
        }
    )

def test_eventbridge_target():
    """Test that the EventBridge rule has the Step Function state machine as a target."""
    app = cdk.App()
    stack = DataMeshPipelineStack(app, 'DataMeshPipelineStack')
    template = assertions.Template.from_stack(stack)

    # Find the EventBridge Rule
    resources = template.find_resources("AWS::Events::Rule")
    assert len(resources) == 1, "Expected one EventBridge Rule"

    rule = next(iter(resources.values()))
    targets = rule['Properties'].get('Targets', [])
    assert len(targets) > 0, "EventBridge Rule has no targets"

    # Check that the target is the Step Functions state machine
    for target in targets:
        arn = target['Arn']
        resource_name = None
        if isinstance(arn, dict):
            if 'Fn::GetAtt' in arn:
                resource_name = arn['Fn::GetAtt'][0]
            elif 'Ref' in arn:
                resource_name = arn['Ref']
            elif 'Fn::Join' in arn:
                # Handle Fn::Join if present
                join_parts = arn['Fn::Join'][1]
                resource_name = ''.join([str(part) if isinstance(part, str) else '' for part in join_parts])
            else:
                # Handle other intrinsic functions if necessary
                resource_name = ''
        elif isinstance(arn, str):
            resource_name = arn

        assert resource_name is not None, "Unable to determine the target resource name from Arn"
        assert 'GlueStateMachine' in resource_name, f"EventBridge target '{resource_name}' is not the Step Functions state machine"
