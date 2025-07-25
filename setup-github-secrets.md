# Production API Integration Setup

## Required Configuration for Automated ETL

To enable the automated data pipeline, configure these production API credentials in your GitHub repository:

### 1. Navigate to Repository Settings
- Go to your GitHub repository
- Click **Settings** tab
- Click **Secrets and variables** → **Actions**

### 2. Add Required Secrets

Click **New repository secret** and add each of these:

#### SUPABASE_URL
```
Name: SUPABASE_URL
Value: https://your-project-id.supabase.co
```
*Your production database API endpoint*

#### SUPABASE_SERVICE_KEY  
```
Name: SUPABASE_SERVICE_KEY
Value: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```
*Service role key with read access to analytics tables*

### 3. Verify Setup

Once secrets are added, the GitHub Actions will:

- ✅ **2-Hourly ETL**: Automated data pipeline updates
- ✅ **Code Validation**: Tests pipeline changes before deployment  
- ✅ **Quality Monitoring**: Automated data validation and alerting

### 4. Production Data Source

This portfolio project demonstrates:
- **Live SaaS Platform**: Real user analytics from production system
- **Multi-tool Environment**: 28+ different processing capabilities
- **Active User Base**: 500+ daily transactions processed
- **Enterprise Architecture**: Production-grade data engineering

### 5. Monitor Pipeline Health

GitHub Actions dashboard provides:
- ✅ Real-time pipeline status and execution logs
- 📊 Automated data quality reports and validation
- 🗃️ Database artifacts and backup management
- 📈 Performance metrics and execution history

## Security Notes

- ⚠️ **Service key has admin access** - keep it secret
- 🔒 **GitHub encrypts secrets** - they're safe in the repository
- 👥 **Only repository admins** can view/edit secrets

---

*Once setup is complete, your data warehouse will automatically stay fresh with daily updates from your Supabase production data!*