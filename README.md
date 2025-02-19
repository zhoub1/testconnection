### **AWS DataZone CDK Deployment**  

This project provides a highly configurable AWS Cloud Development Kit (CDK) solution for deploying AWS DataZone environments. It streamlines **data governance, access control, and metadata management** by integrating key AWS services such as **S3, Glue, IAM, and Lake Formation**.  

With automated infrastructure provisioning, least-privilege IAM enforcement, and pre-configured Glue Crawlers, this setup is designed for efficiency, scalability, and security.  

---

## **Key Features**  
- **Automated DataZone Deployment**: Creates a full AWS DataZone setup, including domain, project, and environment.  
- **IAM Role and Lake Formation Integration**: Configures access controls with **least privilege permissions** while ensuring compatibility with AWS DataZone.  
- **Automated Data Discovery**: Uses AWS Glue Crawlers to catalog and update datasets in DataZone.  
- **Customizable Blueprints**: Supports environment-specific DataZone blueprints for tailored deployments.  
- **Preconfigured Security Policies**: Ensures encryption, access controls, and structured IAM policies.  
- **Automated Cleanup**: Helps prevent orphaned resources and unnecessary costs.  

---

## **Prerequisites**  
Before deploying, ensure the following:  
- **AWS Account** with administrative privileges.  
- **AWS CLI** installed and configured (`aws configure`).  
- **Node.js** installed for AWS CDK ([Install Node.js](https://nodejs.org/en/download/)).  
- **AWS CDK Toolkit** installed (`npm install -g aws-cdk`).  
- **Python 3.7+** installed for dependency management.  

---

## **Deployment Configuration**  
The CDK stack allows customization via `cdk.json` or command-line parameters:  

| Parameter | Description |  
|-----------|------------|  
| `domain_name` | Unique AWS DataZone domain name. |  
| `domain_description` | Description of the DataZone domain. |  
| `project_name` | Lowercase alphanumeric project name. |  
| `environment_name` | Lowercase-only DataZone environment name. |  
| `environment_profile_name` | Environment profile for DataZone (e.g., "Data Lake Blueprint"). |  
| `glue_crawler_schedule` | Cron expression for Glue Crawler (default: daily at 8 AM UTC). |  
| `data_source_schedule` | Cron schedule for DataZone data source ingestion (default: daily at 9 AM UTC). |  
| `project_owner_identifier` | IAM ARN or SSO ID of the DataZone project owner. |  

### **Deploy with Custom Parameters**  
Modify `cdk.json` or pass parameters dynamically:  
```bash
cdk deploy -c domain_name=mydatazone -c glue_crawler_schedule="cron(0 7 * * ? *)"
```  

---

## **Deployment Guide**  

1. **Clone the Repository**  
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```  

2. **Install Dependencies**  
   ```bash
   pip install -r requirements.txt
   ```  

3. **Bootstrap AWS CDK**  
   ```bash
   cdk bootstrap
   ```  

4. **Synthesize the CloudFormation Template**  
   ```bash
   cdk synth
   ```  

5. **Deploy AWS DataZone**  
   ```bash
   cdk deploy
   ```  

The AWS DataZone domain, project, and environment will be deployed automatically.  

---

## **Important: Assigning Lake Formation Admin Permissions**  
Since AWS DataZone interacts with **Lake Formation**, any IAM role responsible for deploying or managing DataZone **must be explicitly added** as an admin in **Lake Formation Settings**.  

### **Manual Steps (AWS Console)**  
1. Navigate to `AWS Lake Formation` → **Data Lake Settings** → **Admins**.  
2. Add the required IAM role:  
   ```
   arn:aws:iam::<AWS_ACCOUNT_ID>:role/<ROLE_NAME>
   ```
3. Save the settings and confirm the role is listed.  

### **Automated Setup**  
For automation, use the script located in `dz_scripts/`, which streamlines role assignment for multiple users and roles.  

**Script Location:**  
```bash
python dz_scripts/add_lakeformation_admin.py --role <ROLE_ARN>
```

The script ensures necessary roles are properly registered with Lake Formation, reducing manual configuration effort.  

---

## **Security Best Practices**  
- **Least Privilege IAM Roles**: Assign only necessary permissions to IAM roles.  
- **S3 Encryption**: Ensure sensitive data is encrypted in transit and at rest.  
- **Regular IAM and Lake Formation Audits**: Review access policies periodically.  
- **Remove Unused Resources**: Clean up old DataZone projects and environments.  

---

## **Resource Cleanup**  
To **remove** the deployed AWS DataZone stack:  
```bash
cdk destroy
```  
**Note:** Some resources, such as databases, Athena workgroups, and logs, may persist. These must be deleted manually.  

---

## **Troubleshooting**  
### **Common Issues and Fixes**  

| Issue | Resolution |  
|-------|------------|  
| Lake Formation permission errors | Ensure the IAM role managing DataZone is added as a **Lake Formation admin**. |  
| S3 bucket name conflicts | Modify `project_name` to create a unique bucket name. |  
| Glue Crawler not running | Verify the `glue_crawler_schedule` and manually trigger it in AWS Glue. |  
| Deployment fails on IAM role creation | Ensure no conflicting IAM roles exist. Delete or rename duplicates. |  

---

## **Running Tests**  
This project includes a **pytest suite** to validate AWS DataZone deployment.  

1. **Install test dependencies**  
   ```bash
   pip install -r requirements.txt
   ```  

2. **Run tests**  
   ```bash
   pytest tests/
   ```  

This verifies:  
- IAM Role Configuration  
- S3 Bucket Policies  
- Glue Crawler Integration  
- DataZone Domain and Environment Deployment  

---

## **Additional Resources**  
- [AWS DataZone Documentation](https://docs.aws.amazon.com/datazone)  
- [AWS CDK Documentation](https://docs.aws.amazon.com/cdk/v2/guide/home.html)  
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/latest/dg/what-is-glue.html)  

---

## **Contributors**  
- Bill Zhou  
- Justin Miles  

This project is continuously maintained and optimized for secure, scalable DataZone deployments.

