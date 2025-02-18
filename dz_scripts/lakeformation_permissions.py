import boto3
import json
import logging
import argparse
import os
from botocore.exceptions import BotoCoreError, ClientError

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

DEFAULT_JSON_FILE = "input.json"

def init_client(service, region=None):
    """Initialize an AWS client with optional region."""
    return boto3.client(service, region_name=region)

def get_existing_admins(lf_client):
    """Retrieve current Data Lake Administrators."""
    try:
        response = lf_client.get_data_lake_settings()
        return [admin["DataLakePrincipalIdentifier"] for admin in response.get("DataLakeSettings", {}).get("DataLakeAdmins", [])]
    except (BotoCoreError, ClientError) as e:
        logging.error(f"Failed to fetch Data Lake Admins: {e}")
        return []

def register_admins(lf_client, users, roles):
    """Add new users and roles to Data Lake Admins without overwriting existing ones."""
    logging.info("Registering Data Lake Administrators...")
    existing_admins = get_existing_admins(lf_client)
    new_admins = list(set(existing_admins + users + roles))
    try:
        lf_client.put_data_lake_settings(DataLakeSettings={"DataLakeAdmins": [{"DataLakePrincipalIdentifier": arn} for arn in new_admins]})
        logging.info("Successfully updated Data Lake Admins.")
    except (BotoCoreError, ClientError) as e:
        logging.error(f"Failed to register admins: {e}")

def grant_permissions(lf_client, principal, resource, permissions):
    """Grant permissions on a specified LakeFormation resource."""
    try:
        lf_client.grant_permissions(
            Principal={"DataLakePrincipalIdentifier": principal},
            Resource=resource,
            Permissions=permissions,
        )
        logging.info(f"Granted {permissions} to {principal} on {resource}.")
    except (BotoCoreError, ClientError) as e:
        logging.error(f"Error granting permissions: {e}")

def process_permissions(lf_client, users, roles, databases):
    """Apply permissions for users/roles on databases and tables."""
    for database in databases:
        db_name = database["name"]
        resource = {"Database": {"Name": db_name}}
        for principal in users + roles:
            grant_permissions(lf_client, principal, resource, ["ALL"])
            
        for table in database.get("tables", []):
            table_resource = {"Table": {"DatabaseName": db_name, "Name": table}}
            for principal in users + roles:
                grant_permissions(lf_client, principal, table_resource, ["SELECT", "DESCRIBE"])

def main(json_file, region):
    """Main function to execute script."""
    if not os.path.exists(json_file):
        logging.error(f"Configuration file {json_file} not found.")
        return
    
    with open(json_file, "r") as f:
        config = json.load(f)
    
    users = config.get("users", [])
    roles = config.get("roles", [])
    databases = config.get("databases", [])
    
    lf_client = init_client("lakeformation", region)
    register_admins(lf_client, users, roles)
    process_permissions(lf_client, users, roles, databases)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Apply LakeFormation Permissions")
    parser.add_argument("json_file", nargs='?', default=DEFAULT_JSON_FILE, help="Path to JSON config file (default: input.json)")
    parser.add_argument("--region", help="AWS region", default=None)
    args = parser.parse_args()
    
    main(args.json_file, args.region)
