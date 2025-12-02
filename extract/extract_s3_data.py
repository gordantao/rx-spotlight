"""
Extraction module for loading raw Medicare Part D data from S3
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import config


def get_spark_session() -> SparkSession:
    """
    Initialize and return a Spark session configured for S3 access
    """
    builder = SparkSession.builder
    
    # Apply all Spark configurations
    for key, value in config.SPARK_CONFIG.items():
        builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def extract_partd_provider_drug(spark: SparkSession, year: str = None) -> DataFrame:
    """
    Extract Medicare Part D prescriber data at provider-drug level
    Main dataset with 100M+ rows
    
    Args:
        spark: Active Spark session
        year: Optional year filter (e.g., '2023', '2024')
    
    Returns:
        DataFrame with prescription data
    """
    path = config.RAW_PARTD_PROVIDER_DRUG
    if year:
        path = f"{path}{year}/"
    
    print(f"📥 Extracting Part D Provider-Drug data from {path}")
    
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("mode", "DROPMALFORMED")
        .csv(path)
    )
    
    print(f"✅ Loaded {df.count():,} records from provider_drug dataset")
    
    return df


def extract_npi_registry(spark: SparkSession) -> DataFrame:
    """
    Extract NPI Registry data for provider specialty and taxonomy information
    
    Args:
        spark: Active Spark session
    
    Returns:
        DataFrame with NPI provider information
    """
    path = config.RAW_NPI_REGISTRY
    
    print(f"📥 Extracting NPI Registry data from {path}")
    
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    )
    
    print(f"✅ Loaded {df.count():,} NPI records")
    
    return df


def extract_census_data(spark: SparkSession) -> DataFrame:
    """
    Extract Census population data for geographic normalization
    
    Args:
        spark: Active Spark session
    
    Returns:
        DataFrame with population by geography
    """
    path = config.RAW_CENSUS
    
    print(f"📥 Extracting Census data from {path}")
    
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    )
    
    print(f"✅ Loaded Census data")
    
    return df


def extract_drug_reference(spark: SparkSession) -> DataFrame:
    """
    Extract drug reference data including opioid classifications
    
    Args:
        spark: Active Spark session
    
    Returns:
        DataFrame with drug classifications
    """
    path = config.RAW_DRUG_REFERENCE
    
    print(f"📥 Extracting Drug Reference data from {path}")
    
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    )
    
    print(f"✅ Loaded drug reference data")
    
    return df


def extract_partd_provider_summary(spark: SparkSession, year: str = None) -> DataFrame:
    """
    Extract provider-level aggregate data
    
    Args:
        spark: Active Spark session
        year: Optional year filter
    
    Returns:
        DataFrame with provider summary
    """
    path = config.RAW_PARTD_PROVIDER
    if year:
        path = f"{path}{year}/"
    
    print(f"📥 Extracting Part D Provider Summary from {path}")
    
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    )
    
    print(f"✅ Loaded provider summary data")
    
    return df


def extract_partd_geography_drug(spark: SparkSession, year: str = None) -> DataFrame:
    """
    Extract geographic-level prescription aggregates
    
    Args:
        spark: Active Spark session
        year: Optional year filter
    
    Returns:
        DataFrame with geographic prescription data
    """
    path = config.RAW_PARTD_GEOGRAPHY_DRUG
    if year:
        path = f"{path}{year}/"
    
    print(f"📥 Extracting Part D Geography-Drug data from {path}")
    
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    )
    
    print(f"✅ Loaded geographic prescription data")
    
    return df


def extract_all_datasets(spark: SparkSession, year: str = None) -> dict:
    """
    Extract all required datasets for the ETL pipeline
    
    Args:
        spark: Active Spark session
        year: Optional year filter for Part D data
    
    Returns:
        Dictionary of DataFrames
    """
    print("\n" + "="*70)
    print("🚀 Starting Data Extraction from S3 Data Lake")
    print("="*70 + "\n")
    
    datasets = {
        'provider_drug': extract_partd_provider_drug(spark, year),
        'npi_registry': extract_npi_registry(spark),
        'census': extract_census_data(spark),
        'drug_reference': extract_drug_reference(spark),
        'provider_summary': extract_partd_provider_summary(spark, year),
        'geography_drug': extract_partd_geography_drug(spark, year)
    }
    
    print("\n" + "="*70)
    print("✅ Data Extraction Complete")
    print("="*70 + "\n")
    
    return datasets


if __name__ == "__main__":
    # Test extraction
    spark = get_spark_session()
    datasets = extract_all_datasets(spark, year="2023")
    
    # Show sample data
    print("\n📊 Sample Provider-Drug Data:")
    datasets['provider_drug'].show(5, truncate=False)
