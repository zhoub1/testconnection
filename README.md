## AWS DataZone CDK Deployment

This project provides a highly configurable AWS Cloud Development Kit (CDK) solution to simplify and accelerate the deployment of AWS DataZone environments. By automating core DataZone components alongside services like S3, Glue, IAM, and Lake Formation, this CDK project delivers a sleek, modern foundation for data management, governance, and analytics.

---

### Overview

The AWS DataZone CDK solution makes deploying a scalable data zone fast and consistent. It integrates essential AWS services—including Amazon S3, AWS Glue, IAM, and Lake Formation—to create an environment optimized for data management and governance.

---

### Key Features

- **DataZone Domain, Project, and Environment**: Easily establishes foundational DataZone structures for streamlined data asset governance.
- **S3 Buckets**: Automatically provisions S3 buckets for raw, processed, and system metadata storage.
- **AWS Glue Integration**: Automates data discovery and cataloging with AWS Glue Crawlers to keep data up to date.
- **IAM Roles and Permissions**: Configures least privilege IAM roles, enhancing security and minimizing risks.
- **Lake Formation Permissions**: Manages Lake Formation access controls for unified, consistent data governance.
- **Custom Environment Blueprints**: Leverages customizable blueprints to extend and tailor environments for specific use cases.
- **Automated Resource Cleanup**: Cleans up resources upon deletion, preventing orphaned assets and reducing costs.

---

### Prerequisites

- **AWS Account**: An AWS account with administrative permissions is required.
- **AWS CLI**: Make sure the AWS CLI is set up and configured for your account.
- **Node.js**: Install Node.js to use the AWS CDK. [Install Node.js](https://nodejs.org/en/download/).
- **CDK Toolkit**: Install the AWS CDK Toolkit. Refer to the [AWS CDK documentation](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html).
- **Python**: Python 3.7+ is needed for managing AWS CDK projects.
- **Environment Variables**: Set these environment variables for default deployment configurations:
  - `CDK_DEFAULT_ACCOUNT`: Your AWS account ID.
  - `CDK_DEFAULT_REGION`: The AWS region for deployment.

---

### Deployment Configuration

Customize the CDK stack using context parameters, which can be set in `cdk.json` or directly via the command line:

- **domain_name**: Unique identifier for the DataZone domain.
- **domain_description**: Short description of the domain's purpose.
- **project_name**: Name of the DataZone project (lowercase alphanumeric).
- **environment_name**: Name of the DataZone environment (lowercase letters).
- **environment_profile_name**: Environment profile name (e.g., "Data Lake Blueprint").
- **glue_crawler_schedule**: Cron schedule for Glue Crawler (default: daily at 8 AM UTC).
- **data_source_schedule**: Cron schedule for DataZone DataSource (default: daily at 9 AM UTC).

To deploy with custom parameters, modify `cdk.json` or pass parameters via the command line:

```bash
cdk deploy -c domain_name=mydomain -c glue_crawler_schedule="cron(0 7 * * ? *)"
```

---

### Quickstart Deployment Guide

1. **Clone the Repository**:

   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Install Dependencies**:

   Install required Python packages from `requirements.txt`.
   
   ```bash
   pip install -r requirements.txt
   ```

3. **Bootstrap AWS Environment**:

   Set up your AWS environment for deploying CDK apps.

   ```bash
   cdk bootstrap
   ```

4. **Synthesize the CloudFormation Template**:

   Generate the CloudFormation template to validate your CDK configuration.
   
   ```bash
   cdk synth
   ```

5. **Deploy the CDK Stack**:

   Deploy the stack using the following command:

   ```bash
   cdk deploy
   ```

**Note**: Deployment may take several minutes. Ensure all environment variables are set correctly to avoid issues.

---

### Customization

- **Core Configuration Adjustments**: Modify configurations such as domain name, project name, and schedules in `cdk_datazone_stack.py`.
- **IAM Role Customization**: Adhere to least privilege principles and conduct regular audits to maintain security.
- **Deploy Custom Blueprints**: Use the sample CloudFormation template in `datazone-cdk/lib` to extend deployment with custom environment blueprints.

### Custom Environment Blueprint

The `datazone-cdk/lib` directory contains a CloudFormation template for deploying custom DataZone environment blueprints, ideal for testing and specialized configurations.

#### Key Components in the Template

- **Parameters**:
  - `ExistingDomainId`: DataZone domain ID.
  - `S3BucketName`: S3 bucket for testing actions.
  - `DZEnvironment`: DataZone environment identifier.
  - `DZProject`: DataZone project identifier.

- **Resources**:
  - **EnvActionRole**: IAM role (`EnvironmentActionBYORRole`) providing permissions for DataZone service interactions.
  - **EnableBlueprint**: Custom environment blueprint using `AWS::DataZone::EnvironmentBlueprintConfiguration`.
  - **Environment**: Deploys a DataZone environment using the custom blueprint.
  - **BasicEnvironmentAction**: Creates an S3 environment action linked to the bucket for easier management.

#### Example Use Case

Tailor DataZone environments with specific IAM roles and S3 interactions to fit advanced configurations.

---

### Security Best Practices

- **Least Privilege Principle**: Grant only necessary permissions to IAM roles.
- **Encryption**: Enable S3 server-side encryption for sensitive data.
- **Access Control Audits**: Regularly review Lake Formation and IAM permissions to maintain security.

---

### Resource Cleanup and Deletion

To delete the deployed DataZone environment:

```bash
cdk destroy
```

**Note**: Manually delete specific resources—such as databases, Athena workgroups, and CloudWatch log groups—before running `cdk destroy`. These resources are retained by default and must be cleared manually.

---

### Troubleshooting Tips

- **Permission Issues**: Ensure IAM roles have the necessary permissions for managing resources like S3, Glue, and Lake Formation.
- **Resource Limits**: Check if you have reached AWS resource limits.
- **Stack Rollback**: For deployment errors, check the CloudFormation console for detailed error messages.

---
### Running Tests

To ensure the stability and correctness of the DataZone CDK deployment, comprehensive unit tests have been added to validate different components of the CDK stack.

#### Running the Tests

1. **Install Test Dependencies** (if not already installed):

   ```bash
   pip install -r requirements.txt
   ```

2. **Run Tests** using `pytest`:

   ```bash
   pytest tests/
   ```

   This command will execute all test cases located in the `tests/` directory, ensuring that your CDK components are correctly implemented and functioning as expected.

#### Test Coverage
- **IAM Role Validation**: Confirms the existence of at least five IAM roles and verifies the correct AssumeRole policy.
- **Glue Crawler Properties**: Ensures that Glue crawlers have correct role, database name, schedule, and S3 target paths.
- **Lambda Function Creation**: Validates the creation of specific Lambda functions with the expected properties.
- **S3 Buckets and Encryption**: Checks the properties of S3 buckets, ensuring server-side encryption is enabled.
- **Outputs Verification**: Asserts that all required CloudFormation outputs are present.
- **DataZone Components**: Validates the creation of essential DataZone domain, project, and environment.

All tests are documented and were co-authored by Bill Zhou and Justin Miles to ensure robustness, accuracy, and quality.

---
### Useful Commands

- **Synthesize CloudFormation Template**:
  ```bash
  cdk synth
  ```
- **Deploy Stack**:
  ```bash
  cdk deploy
  ```
- **Destroy Stack**:
  ```bash
  cdk destroy
  ```
- **Compare Changes**: Use `cdk diff` to compare your stack with the current deployed state.
  ```bash
  cdk diff
  ```

---

### Important Notes

- **DataZone-Env Cleanup**: Manually delete the `DataZone-Env` CloudFormation template before running `cdk destroy`.
- **AWS Support**: If issues arise beyond routine fixes, reach out to AWS Support for assistance.

---

### Additional Resources

- [AWS DataZone Documentation](https://docs.aws.amazon.com/datazone)
- [AWS CDK Documentation](https://docs.aws.amazon.com/cdk/v2/guide/home.html)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html)
