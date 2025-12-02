"""
Configuration settings for Medicare Part D ETL Pipeline
"""

# S3 Bucket Configuration
S3_BUCKET = "s3://medicare-partd-analytics"

# Raw Data Paths
RAW_PARTD_PROVIDER_DRUG = f"{S3_BUCKET}/raw/partd/provider_drug/"
RAW_PARTD_PROVIDER = f"{S3_BUCKET}/raw/partd/provider/"
RAW_PARTD_GEOGRAPHY_DRUG = f"{S3_BUCKET}/raw/partd/geography_drug/"
RAW_NPI_REGISTRY = f"{S3_BUCKET}/raw/npi/"
RAW_CENSUS = f"{S3_BUCKET}/raw/census/"
RAW_DRUG_REFERENCE = f"{S3_BUCKET}/raw/drug_reference/"

# Processed Data Paths
PROCESSED_BASE = f"{S3_BUCKET}/processed/"
PROCESSED_CLEAN = f"{PROCESSED_BASE}clean/"
PROCESSED_AGGREGATED = f"{PROCESSED_BASE}aggregated/"
PROCESSED_HOTSPOTS = f"{PROCESSED_BASE}hotspots/"
PROCESSED_TEMPORAL = f"{PROCESSED_BASE}temporal/"
PROCESSED_TABLEAU = f"{PROCESSED_BASE}tableau_ready/"

# Spark Configuration
SPARK_CONFIG = {
    "spark.app.name": "Medicare-PartD-ETL",
    "spark.executor.memory": "8g",
    "spark.driver.memory": "4g",
    "spark.executor.cores": "4",
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
}

# High-Risk Drug Categories
OPIOID_KEYWORDS = [
    'OXYCODONE', 'HYDROCODONE', 'FENTANYL', 'MORPHINE', 'CODEINE',
    'TRAMADOL', 'METHADONE', 'HYDROMORPHONE', 'OXYMORPHONE', 'BUPRENORPHINE',
    'TAPENTADOL', 'MEPERIDINE', 'PERCOCET', 'VICODIN', 'NORCO'
]

BENZODIAZEPINE_KEYWORDS = [
    'ALPRAZOLAM', 'DIAZEPAM', 'LORAZEPAM', 'CLONAZEPAM', 'TEMAZEPAM',
    'XANAX', 'VALIUM', 'ATIVAN', 'KLONOPIN'
]

STIMULANT_KEYWORDS = [
    'AMPHETAMINE', 'DEXTROAMPHETAMINE', 'METHYLPHENIDATE', 'LISDEXAMFETAMINE',
    'ADDERALL', 'RITALIN', 'VYVANSE', 'CONCERTA'
]

# Column Mappings for Standardization
PARTD_PROVIDER_DRUG_SCHEMA = {
    'npi': 'npi',
    'nppes_provider_last_org_name': 'provider_name',
    'nppes_provider_first_name': 'provider_first_name',
    'nppes_provider_city': 'provider_city',
    'nppes_provider_state': 'provider_state',
    'nppes_provider_zip5': 'provider_zip',
    'specialty_description': 'specialty',
    'drug_name': 'drug_name',
    'generic_name': 'generic_name',
    'total_claim_count': 'total_claims',
    'total_30_day_fill_count': 'total_fills',
    'total_day_supply': 'total_day_supply',
    'total_drug_cost': 'total_cost',
    'bene_count': 'beneficiary_count'
}

# Data Quality Thresholds
MIN_CLAIM_COUNT = 11  # CMS suppresses values < 11 for privacy
MAX_COST_PER_CLAIM = 50000  # Flag suspicious claims above this
MIN_BENEFICIARIES = 1

# Geographic Aggregation Levels
GEO_LEVELS = ['state', 'zip', 'county']

# Temporal Aggregation Levels
TEMPORAL_LEVELS = ['year', 'quarter', 'month']
