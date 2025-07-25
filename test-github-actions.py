#!/usr/bin/env python3
"""
Local test script for GitHub Actions ETL pipeline
Simulates the GitHub Actions environment and workflow
"""

import os
import sys
import subprocess
from datetime import datetime

def run_command(cmd, description):
    """Run a command and show results"""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ {description} - SUCCESS")
            if result.stdout.strip():
                print(f"   Output: {result.stdout.strip()}")
            return True
        else:
            print(f"❌ {description} - FAILED")
            if result.stderr.strip():
                print(f"   Error: {result.stderr.strip()}")
            return False
    except Exception as e:
        print(f"❌ {description} - ERROR: {e}")
        return False

def check_environment():
    """Check if environment is set up correctly"""
    print("🔍 ENVIRONMENT VALIDATION")
    print("=" * 30)
    
    # Check Python version
    python_version = sys.version.split()[0]
    print(f"🐍 Python version: {python_version}")
    
    # Check required environment variables
    required_vars = ['SUPABASE_URL', 'SUPABASE_SERVICE_KEY']
    missing_vars = []
    
    for var in required_vars:
        if os.getenv(var):
            print(f"✅ {var}: Set")
        else:
            print(f"❌ {var}: Missing")
            missing_vars.append(var)
    
    if missing_vars:
        print(f"\n⚠️  Missing environment variables: {', '.join(missing_vars)}")
        print("   Please set them in your .env file")
        return False
    
    return True

def test_etl_workflow():
    """Test the main ETL workflow steps"""
    print("\n🚀 TESTING ETL WORKFLOW")
    print("=" * 25)
    
    steps = [
        ("python -m py_compile dashboard_star_schema_etl.py", "Validate ETL script syntax"),
        ("python -c 'from dashboard_star_schema_etl import DashboardStarSchemaETL; print(\"Import successful\")'", "Test ETL imports"),
        ("mkdir -p data", "Create data directory"),
        ("python dashboard_star_schema_etl.py", "Run full ETL pipeline"),
    ]
    
    all_passed = True
    for cmd, desc in steps:
        if not run_command(cmd, desc):
            all_passed = False
            break
    
    return all_passed

def test_data_quality():
    """Test data quality checks"""
    print("\n🔍 TESTING DATA QUALITY")
    print("=" * 23)
    
    quality_script = """
import duckdb
import os

if not os.path.exists('data/dashboard_analytics.duckdb'):
    print('❌ Database file not found')
    exit(1)

conn = duckdb.connect('data/dashboard_analytics.duckdb')

# Check table counts
tables = ['fact_analytics', 'fact_daily_kpis', 'dim_tools', 'dim_time']
for table in tables:
    count = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
    print(f'✅ {table}: {count} records')

# Check latest data
latest = conn.execute('SELECT MAX(created_at) FROM fact_analytics').fetchone()[0]
print(f'✅ Latest data: {latest}')

conn.close()
print('✅ Data quality validation passed')
"""
    
    return run_command(f"python -c \"{quality_script}\"", "Run data quality checks")

def simulate_github_actions():
    """Simulate the complete GitHub Actions workflow"""
    print("🤖 SIMULATING GITHUB ACTIONS WORKFLOW")
    print("=" * 40)
    print(f"📅 Simulation Date: {datetime.now()}")
    print()
    
    # Step 1: Environment check
    if not check_environment():
        print("\n❌ SIMULATION FAILED: Environment not ready")
        return False
    
    # Step 2: ETL workflow test
    if not test_etl_workflow():
        print("\n❌ SIMULATION FAILED: ETL workflow failed")
        return False
    
    # Step 3: Data quality test
    if not test_data_quality():
        print("\n❌ SIMULATION FAILED: Data quality checks failed")
        return False
    
    print("\n🎉 GITHUB ACTIONS SIMULATION PASSED!")
    print("=" * 35)
    print("✅ All workflow steps completed successfully")
    print("✅ ETL pipeline is ready for GitHub Actions")
    print("✅ Data quality validation passed")
    print("\n📋 NEXT STEPS:")
    print("1. Commit your changes to GitHub")
    print("2. Set up GitHub secrets (see setup-github-secrets.md)")
    print("3. GitHub Actions will run automatically")
    
    return True

if __name__ == "__main__":
    # Load environment variables from .env file
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        print("💡 Install python-dotenv for .env file support: pip install python-dotenv")
    
    success = simulate_github_actions()
    sys.exit(0 if success else 1)