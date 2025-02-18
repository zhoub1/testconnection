import unittest
import sys
import os

# Add the parent directory to the Python path to allow imports from the 'lib' directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from aws_cdk import App, Environment
from aws_cdk.assertions import Template, Match
from lib.cdk_datazone_stack import CdkDatazoneStack

class TestCdkDatazoneStack(unittest.TestCase):
    """
    Unit tests for the CdkDatazoneStack AWS CDK stack.

    This test suite verifies that all necessary AWS resources are created with the expected configurations.
    
    **Authors:**  
    - Bill Zhou  
    - Justin Miles
    """

    def setUp(self):
        """
        Set up the test environment by initializing the CDK app and stack.

        This method runs before each test case, ensuring a fresh stack for isolation.
        """
        # Define the AWS environment (account and region)
        env = Environment(account='123456789012', region='us-east-1')
        
        # Initialize the CDK app and set the required context variable
        app = App()
        app.node.set_context("project_owner_identifier", "arn:aws:iam::268072525263:user/zhoub1")
        
        # Instantiate the stack
        self.stack = CdkDatazoneStack(app, "CdkDatazoneStack", env=env)
        
        # Create an assertions template from the stack
        self.template = Template.from_stack(self.stack)

    def test_s3_buckets_created(self):
        """
        Test that exactly two S3 buckets are created with the expected encryption and versioning configurations.

        **Assertions:**
        - Two S3 buckets exist.
        - Both buckets have AES256 server-side encryption enabled.
        - At least one bucket has versioning enabled.
        """
        # Assert that exactly two S3 buckets are present
        self.template.resource_count_is("AWS::S3::Bucket", 2)

        # Define expected BucketEncryption properties
        expected_encryption = {
            "BucketEncryption": {
                "ServerSideEncryptionConfiguration": [{
                    "ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}
                }]
            }
        }

        # Retrieve all S3 bucket resources
        s3_buckets = self.template.find_resources("AWS::S3::Bucket")
        versioning_found = False

        for bucket_id, bucket in s3_buckets.items():
            with self.subTest(bucket=bucket_id):
                # Check for BucketEncryption
                self.assertIn(
                    "BucketEncryption",
                    bucket["Properties"],
                    f"Bucket '{bucket_id}' is missing the 'BucketEncryption' property."
                )
                self.assertEqual(
                    bucket["Properties"]["BucketEncryption"],
                    expected_encryption["BucketEncryption"],
                    f"Bucket '{bucket_id}' 'BucketEncryption' does not match the expected configuration."
                )

                # Check for VersioningConfiguration if it exists
                if "VersioningConfiguration" in bucket["Properties"]:
                    self.assertEqual(
                        bucket["Properties"]["VersioningConfiguration"],
                        {"Status": "Enabled"},
                        f"Bucket '{bucket_id}' 'VersioningConfiguration' is not enabled as expected."
                    )
                    versioning_found = True

        # Ensure that at least one bucket has versioning enabled
        self.assertTrue(
            versioning_found,
            "At least one S3 bucket should have 'VersioningConfiguration' enabled."
        )

    def test_lambda_function_created(self):
        """
        Test that the GlueCrawlerTriggerFunction Lambda function is created with expected properties.

        **Assertions:**
        - The Lambda function has the correct handler.
        - The specified runtime is used.
        - The timeout is set appropriately.
        - Environment variables are configured correctly.
        - The function name matches the expected pattern.
        """
        self.template.has_resource_properties("AWS::Lambda::Function", {
            "Handler": "index.handler",
            "Runtime": "nodejs16.x",
            "Timeout": 60,
            "Environment": {
                "Variables": {
                    "CRAWLER_NAME": Match.any_value()
                }
            },
            "FunctionName": "GlueCrawlerTriggerFunction-datazoneproj-datazoneenv"
        })

    def test_iam_roles_created(self):
        """
        Test that at least five IAM roles are created and that a Lambda execution role exists with the correct AssumeRolePolicyDocument.

        **Assertions:**
        - Five IAM roles exist.
        - One IAM role is specifically for Lambda execution with the appropriate trust policy.
        """
        # Retrieve all IAM Role resources
        iam_roles = self.template.find_resources("AWS::IAM::Role")
        
        # Assert that at least five IAM roles exist
        self.assertGreaterEqual(
            len(iam_roles),
            5,
            "There should be at least five IAM roles."
        )

        # Initialize a flag to check for Lambda execution role
        lambda_role_found = False

        for role_id, role in iam_roles.items():
            assume_role_policy = role.get("Properties", {}).get("AssumeRolePolicyDocument", {})
            statements = assume_role_policy.get("Statement", [])

            for statement in statements:
                if (
                    statement.get("Action") == "sts:AssumeRole" and 
                    statement.get("Effect") == "Allow" and
                    statement.get("Principal", {}).get("Service") == "lambda.amazonaws.com"
                ):
                    lambda_role_found = True
                    break
            if lambda_role_found:
                break

        # Assert that a Lambda execution role exists
        self.assertTrue(
            lambda_role_found,
            "Lambda execution role with correct 'AssumeRolePolicyDocument' not found."
        )

    def test_glue_crawler_created(self):
        """
        Test that exactly one Glue crawler is created with the expected properties and S3 targets.

        **Assertions:**
        - One Glue crawler exists.
        - The crawler has the correct role with proper ARN reference.
        - Associated database name is as expected.
        - Defined schedule is correct.
        - Schema change policy is configured properly.
        - Table prefix is set correctly.
        - Proper configuration is applied.
        - At least one S3 target with correct path is configured.
        """
        # Retrieve all Glue Crawler resources
        glue_crawlers = self.template.find_resources("AWS::Glue::Crawler")
        
        # Assert that exactly one Glue crawler exists
        self.assertEqual(
            len(glue_crawlers),
            1,
            "There should be exactly one Glue crawler."
        )

        # Define the expected properties for the Glue crawler
        expected_properties = {
            "Role": Match.object_like({
                "Fn::GetAtt": [Match.string_like_regexp("^GlueCrawlerRole.*"), "Arn"]
            }),
            "DatabaseName": "datazoneenv_pub_db",
            "Schedule": {
                "ScheduleExpression": "cron(0 8 * * ? *)"
            },
            "SchemaChangePolicy": {
                "DeleteBehavior": "LOG",
                "UpdateBehavior": "UPDATE_IN_DATABASE"
            },
            "TablePrefix": "datazoneenv_pub_",
            "Configuration": Match.any_value()
        }

        # Check properties of the Glue crawler
        self.template.has_resource_properties("AWS::Glue::Crawler", expected_properties)

        # Additionally, verify the S3Targets Path
        for crawler_id, crawler in glue_crawlers.items():
            with self.subTest(crawler=crawler_id):
                targets = crawler["Properties"].get("Targets", {})
                s3_targets = targets.get("S3Targets", [])

                # Assert that at least one S3Target exists
                self.assertGreaterEqual(
                    len(s3_targets),
                    1,
                    "Glue Crawler should have at least one S3Target."
                )

                for s3_target in s3_targets:
                    self.assertIn(
                        "Path",
                        s3_target,
                        "S3Target is missing the 'Path' property."
                    )
                    path = s3_target["Path"]

                    if isinstance(path, dict):
                        # Expect Fn::Join structure: ["s3://", {"Ref": "BucketLogicalId"}, "/"]
                        self.assertIn(
                            "Fn::Join",
                            path,
                            "S3Target 'Path' should be a Fn::Join object."
                        )
                        join = path["Fn::Join"]
                        self.assertIsInstance(
                            join,
                            list,
                            "Fn::Join should be a list."
                        )
                        self.assertEqual(
                            join[0],
                            "",
                            "Fn::Join separator should be an empty string."
                        )
                        join_parts = join[1]
                        self.assertIsInstance(
                            join_parts,
                            list,
                            "Fn::Join parts should be a list."
                        )
                        self.assertGreaterEqual(
                            len(join_parts),
                            3,
                            "Fn::Join parts should have at least three elements."
                        )
                        self.assertEqual(
                            join_parts[0],
                            "s3://",
                            "Fn::Join first part should be 's3://'."
                        )
                        self.assertEqual(
                            join_parts[2],
                            "/",
                            "Fn::Join third part should be '/'."
                        )
                        # The second part should be a Ref to the S3 bucket
                        bucket_ref = join_parts[1]
                        self.assertIsInstance(
                            bucket_ref,
                            dict,
                            "Fn::Join part[1] should be a dictionary with 'Ref'."
                        )
                        self.assertIn(
                            "Ref",
                            bucket_ref,
                            "Fn::Join part[1] should contain 'Ref'."
                        )
                    elif isinstance(path, str):
                        # If Path is a string, ensure it contains the expected bucket name
                        self.assertIn(
                            "datazoneproj-us-east-1",
                            path,
                            "S3Target 'Path' does not contain the expected bucket name."
                        )
                    else:
                        self.fail("S3Target 'Path' is neither a string nor an object.")

    def test_datazone_resources_created(self):
        """
        Test that the DataZone resources (Domain, Project, Environment) are created.

        **Assertions:**
        - One DataZone Domain exists.
        - One DataZone Project exists.
        - One DataZone Environment exists.
        """
        # Check for DataZone Domain
        self.template.resource_count_is("AWS::DataZone::Domain", 1)

        # Check for DataZone Project
        self.template.resource_count_is("AWS::DataZone::Project", 1)

        # Check for DataZone Environment
        self.template.resource_count_is("AWS::DataZone::Environment", 1)

    def test_outputs_created(self):
        """
        Test that all expected CloudFormation outputs are present.

        **Assertions:**
        - Outputs include:
            - S3DataSourceBucketName
            - S3DataZoneSysStoreBucketName
            - GlueCrawlerName
            - GlueTablePrefix
            - DataZoneDomainPortalUrl
            - DataZoneDomainId
            - DataZoneExecutionRoleArn
        """
        # Define the expected output keys
        expected_outputs = [
            "S3DataSourceBucketName",
            "S3DataZoneSysStoreBucketName",
            "GlueCrawlerName",
            "GlueTablePrefix",
            "DataZoneDomainPortalUrl",
            "DataZoneDomainId",
            "DataZoneExecutionRoleArn"
        ]

        # Retrieve all output keys from the synthesized template
        actual_outputs = self.template.to_json().get("Outputs", {})
        actual_output_keys = actual_outputs.keys()

        for output in expected_outputs:
            with self.subTest(output=output):
                self.assertIn(
                    output,
                    actual_output_keys,
                    f"Output '{output}' is missing."
                )

if __name__ == '__main__':
    unittest.main()
