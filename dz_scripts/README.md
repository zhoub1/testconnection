# 🚀 AWS Lake Formation Permissions Automation

Easily automate adding **Data Lake Admins** and granting **database/table permissions** in AWS Lake Formation.

---

## ✅ Quick Setup

### 1️⃣ Clone & Install Dependencies
```bash
git clone https://github.com/YOUR_GITHUB_USERNAME/lakeformation-permissions.git
cd lakeformation-permissions
pip install -r requirements.txt
```

### 2️⃣ Configure Permissions (`input.json`)
```json
{
  "users": ["arn:aws:iam::123456789012:user/datazone_admin"],
  "roles": ["arn:aws:iam::123456789012:role/LakeFormationRole"],
  "databases": [{ "name": "target_db", "tables": ["target_table"] }]
}
```

### 3️⃣ Run the Script
```bash
python lakeformation_permissions.py
```

---

## 🔍 Verification Commands
Check if permissions were applied correctly:
```bash
aws lakeformation get-data-lake-settings
aws glue get-database --name target_db
aws glue get-tables --database-name target_db
```

---

## ⚡ Troubleshooting

### ❌ **Access Denied or Resource Not Found?**
Run this to manually set the Data Lake Admin:
```bash
aws lakeformation put-data-lake-settings --cli-input-json '{
  "DataLakeSettings": {
    "DataLakeAdmins": [
      {"DataLakePrincipalIdentifier": "arn:aws:iam::123456789012:user/datazone_admin"}
    ]
  }
}'
```

Need help? **Check AWS IAM permissions and ensure your role has the right policies!** 🔥
