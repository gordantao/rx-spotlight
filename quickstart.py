"""
Quick Start Guide - Running the Medicare Part D ETL Pipeline
"""

# QUICK START GUIDE
# =================

print("""
╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║         Medicare Part D ETL Pipeline - Quick Start Guide            ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝

Prerequisites Checklist:
---------------------------
✓ Python 3.8+ installed
✓ Apache Spark 3.x installed
✓ AWS credentials configured (aws configure)
✓ S3 bucket with Medicare Part D data

Installation Steps:
---------------------

1. Install dependencies:
   pip install -r requirements.txt

2. Configure S3 bucket in config.py:
   S3_BUCKET = "s3://your-bucket-name"

3. Test Spark installation:
   python -c "from pyspark.sql import SparkSession; print('Spark OK')"

Running the Pipeline:
------------------------

OPTION 1: Full Pipeline (Extract → Transform → Load)
   python main.py --year 2023 --mode full

OPTION 2: Individual Phases
   python main.py --year 2023 --mode extract
   python main.py --year 2023 --mode transform
   python main.py --year 2023 --mode load

OPTION 3: With Performance Profiling
   python main.py --year 2023 --mode full --profile

Testing Individual Modules:
------------------------------

Test Extract:
   python extract/extract_s3_data.py

Test Transform:
   python transform/transform_partd.py

Test Load:
   python load/load_to_s3.py

Test Utilities:
   python utils.py

Expected Outputs:
--------------------

After successful execution, check S3 for:

1. processed/tableau_ready/provider_summary/
2. processed/tableau_ready/drug_summary/
3. processed/hotspots/state_level/
4. processed/hotspots/zip_opioid/
5. processed/temporal/yearly_trends/
6. processed/tableau_ready/main_dashboard/

Connecting to Tableau:
-------------------------

1. Open Tableau Desktop
2. Connect → Amazon S3
3. Navigate to: processed/tableau_ready/main_dashboard/
4. Load CSV file
5. Follow TABLEAU_GUIDE.md for dashboard creation

Troubleshooting:
------------------

ERROR: "No module named 'pyspark'"
→ Run: pip install pyspark

ERROR: "Access Denied" (S3)
→ Run: aws configure
→ Check IAM permissions for S3 read/write

ERROR: "Java not found"
→ Install Java 8 or 11
→ Set JAVA_HOME environment variable

ERROR: "Out of memory"
→ Increase Spark memory in config.py:
   "spark.executor.memory": "16g"
   "spark.driver.memory": "8g"

Documentation:
----------------

• README.md - Project overview and setup
• TABLEAU_GUIDE.md - Dashboard creation guide
• config.py - All configuration settings
• utils.py - Utility functions

Pro Tips:
-----------

1. Start with a single year for testing
2. Monitor Spark UI at http://localhost:4040
3. Use --profile flag to identify bottlenecks
4. Check S3 costs (large datasets can be expensive)
5. Use partitioned Parquet for faster Tableau loads

Sample Use Cases:
-------------------

1. Find top opioid-prescribing states:
   SELECT provider_state, SUM(provider_opioid_claims)
   FROM provider_summary
   GROUP BY provider_state
   ORDER BY 2 DESC

2. Identify high-risk providers:
   SELECT provider_name, specialty, opioid_percentage
   FROM provider_summary
   WHERE opioid_percentage > 25
   
3. Analyze geographic hotspots:
   Load state_hotspots dataset in Tableau
   Create heat map by provider_state
   Filter to is_hotspot = 1

📞 Need Help?
------------

• GitHub Issues: https://github.com/gordantao/rx-spotlight/issues
• Email: Contact repository owner

Ready to Start!
-----------------

Run this command to begin:

   python main.py --year 2023 --mode full

═══════════════════════════════════════════════════════════════════════
Happy Analyzing! 🚀
═══════════════════════════════════════════════════════════════════════
""")


# EXAMPLE: Quick data validation check
if __name__ == "__main__":
    import sys
    
    print("\n🔍 Running Quick Environment Check...\n")
    
    # Check Python version
    import sys
    if sys.version_info >= (3, 8):
        print("✅ Python version:", sys.version.split()[0])
    else:
        print("❌ Python 3.8+ required. Current:", sys.version.split()[0])
    
    # Check PySpark
    try:
        from pyspark import __version__
        print(f"PySpark installed: {__version__}")
    except ImportError:
        print("PySpark not installed. Run: pip install pyspark")
    
    # Check Boto3 (AWS SDK)
    try:
        import boto3
        print(f"Boto3 installed: {boto3.__version__}")
    except ImportError:
        print("Boto3 not installed. Run: pip install boto3")
    
    # Check PyArrow (for Parquet)
    try:
        import pyarrow
        print(f"PyArrow installed: {pyarrow.__version__}")
    except ImportError:
        print("PyArrow not installed (optional). Run: pip install pyarrow")
    
    # Check AWS credentials
    try:
        import boto3
        sts = boto3.client('sts')
        account_id = sts.get_caller_identity()["Account"]
        print(f"AWS credentials configured (Account: {account_id})")
    except:
        print("AWS credentials not configured. Run: aws configure")
    
    print("\n" + "="*70)
    print("Environment check complete!")
    print("="*70 + "\n")
