# AWS Glue ETL Pipeline

This project deploys an AWS Glue-based ETL pipeline using AWS CDK (Cloud Development Kit). The pipeline processes CSV files from an S3 input bucket, transforms them using AWS Glue, and writes the transformed data back to an S3 output bucket in Parquet format.
 
## Project Structure

```bash
cdk/
├── README.md                      # Instructions and documentation
├── app.py                         # CDK application entry point
├── requirements.txt               # Python dependencies for the CDK
├── lib/
│   └── data_mesh_pipeline_stack.py  # CDK stack definition
├── scripts/
│   └── glue_script.py             # AWS Glue job script
├── tests/                         # Unit tests for the CDK stack
│   └── test_data_mesh_pipeline_stack.py  # Test suite for stack components
└── cdk.json                       # CDK context (optional) 
```

## Setup Instructions

### 1. Prerequisites

- **AWS CLI** configured with appropriate permissions
- **Python 3.8** or later
- **AWS CDK** installed globally

  ```bash
  npm install -g aws-cdk
  ```

### 2. Install Python Dependencies

Navigate to the project directory and install the required Python packages:

```bash
pip install -r requirements.txt
```

### 3. Deploy the Stack

Run the following commands to bootstrap your AWS environment and deploy the CDK stack:

```bash
cdk bootstrap
cdk synth # Read-only changes, running deploy will also run a synth before it deploys
cdk deploy
```

**Note:** The `cdk deploy` command will display a security-related approval prompt. Review the IAM policy changes and type **'y'** to proceed.

### 4. Test the Pipeline

To test the ETL pipeline:

1. **Upload a test file** to the input S3 bucket created by the stack (e.g., `default-input-bucket` or the name you provided).

2. **Trigger the ETL Process:**
    - The upload will automatically trigger the AWS Glue job via AWS Step Functions and EventBridge.

3. **Verify the Output:**
    - Check the output S3 bucket (e.g., `default-output-bucket/transformed/`) for the transformed Parquet files.

### 5. Monitor the Pipeline

You can monitor the ETL pipeline using:

- **AWS Step Functions Console:** View the execution history and details.
- **AWS Glue Console:** Check the job runs, logs, and metrics.
- **AWS CloudWatch Logs:** Access logs for detailed troubleshooting.

### 6. Run Tests

To ensure the pipeline's stability and correctness, unit tests have been added to validate the functionality of different components of the CDK stack.

#### Running the Tests

1. **Install Test Dependencies** (if not already installed):

   ```bash
   pip install -r requirements.txt
   ```

2. **Run Tests** using `pytest`:

   ```bash
   pytest tests/
   ```

   This command will execute all test cases located in the `tests/` directory, ensuring that your CDK components are correctly implemented and working as expected.

## Configuration

The CDK stack uses context variables for customization. You can set these in the `cdk.json` file.

*Note* that the `data_zone_source_bucket_name` parameter is **optional**. When added to your context file, it overrides the output bucket creation and instead uses the preexisting bucket you provide (typically pointed to the datazone source bucket).

### Example `cdk.json`:

```json
{
  "app": "python3 app.py",
  "context": {
    "glue_input_bucket_name": "glue-datamesh-input-bucket",
    "glue_output_bucket_name": "glue-datamesh-output-bucket",
    "glue_script_bucket_name": "glue-datamesh-transformation-script-cdk",
    "glue_script_key": "glue_script.py",
    "glue_job_name": "glue-datamesh-job",
    "project_name": "data-mesh-pipeline",
    "environment": "development",
    "CDK_DEFAULT_REGION": "us-east-1",
    "CDK_DEFAULT_ACCOUNT": "<INSERTACCOUNTHERE>"
  }
}
```

## Test Coverage

This project includes extensive unit tests to validate the various AWS resources and configurations defined in the CDK stack. The test suite ensures:

- **S3 Buckets** are created with appropriate encryption, versioning, and tags.
- **IAM Roles and Policies** for Glue and Step Functions have necessary permissions.
- **Glue Jobs** are configured correctly, including arguments, workers, and Glue versions.
- **State Machine Definitions** include the necessary Wait states and logging configurations.
- **EventBridge Rules** are configured to trigger state machine executions upon S3 object creation.

## Cleanup

**Important:** Ensure that all data you want to keep is backed up before destroying the stack, as this will delete the S3 buckets and all stored data.

To remove all resources created by the CDK stack, run:

```bash
cdk destroy
```
