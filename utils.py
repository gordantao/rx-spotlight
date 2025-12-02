"""
Utility functions for the Medicare Part D ETL pipeline
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
import logging
from typing import List, Dict


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def validate_dataframe(df: DataFrame, required_columns: List[str]) -> bool:
    """
    Validate that a DataFrame contains required columns
    
    Args:
        df: DataFrame to validate
        required_columns: List of required column names
    
    Returns:
        Boolean indicating validation success
    """
    missing_columns = set(required_columns) - set(df.columns)
    
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    
    logger.info(f"✅ All required columns present: {required_columns}")
    return True


def print_dataframe_summary(df: DataFrame, name: str = "DataFrame") -> None:
    """
    Print comprehensive summary of a DataFrame
    
    Args:
        df: DataFrame to summarize
        name: Name for display purposes
    """
    print(f"\n{'='*70}")
    print(f"📊 {name} Summary")
    print(f"{'='*70}")
    
    # Row count
    count = df.count()
    print(f"\n📈 Total Records: {count:,}")
    
    # Schema
    print(f"\n📋 Schema ({len(df.columns)} columns):")
    df.printSchema()
    
    # Sample data
    print(f"\n🔍 Sample Data (first 5 rows):")
    df.show(5, truncate=False)
    
    # Missing value analysis
    print(f"\n⚠️  Missing Value Analysis:")
    missing_counts = []
    for col in df.columns:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            missing_counts.append((col, null_count, null_count/count*100))
    
    if missing_counts:
        for col, null_count, pct in sorted(missing_counts, key=lambda x: x[1], reverse=True):
            print(f"   • {col}: {null_count:,} ({pct:.2f}%)")
    else:
        print(f"   ✅ No missing values detected")
    
    print(f"{'='*70}\n")


def calculate_data_quality_score(df: DataFrame, required_fields: List[str]) -> float:
    """
    Calculate overall data quality score (0-100)
    
    Args:
        df: DataFrame to evaluate
        required_fields: Critical fields that must be non-null
    
    Returns:
        Data quality score as percentage
    """
    total_records = df.count()
    
    # Count records with all required fields populated
    filter_condition = F.col(required_fields[0]).isNotNull()
    for field in required_fields[1:]:
        filter_condition = filter_condition & F.col(field).isNotNull()
    
    complete_records = df.filter(filter_condition).count()
    
    quality_score = (complete_records / total_records) * 100 if total_records > 0 else 0
    
    logger.info(f"Data Quality Score: {quality_score:.2f}% ({complete_records:,}/{total_records:,})")
    
    return quality_score


def detect_duplicates(df: DataFrame, key_columns: List[str]) -> DataFrame:
    """
    Detect and return duplicate records based on key columns
    
    Args:
        df: DataFrame to check
        key_columns: Columns that define uniqueness
    
    Returns:
        DataFrame containing duplicate records
    """
    duplicates = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col('count') > 1)
        .orderBy(F.col('count').desc())
    )
    
    dup_count = duplicates.count()
    
    if dup_count > 0:
        logger.warning(f"⚠️  Found {dup_count:,} duplicate key combinations")
        duplicates.show(10)
    else:
        logger.info(f"✅ No duplicates found on {key_columns}")
    
    return duplicates


def detect_outliers(df: DataFrame, column: str, threshold: float = 3.0) -> DataFrame:
    """
    Detect statistical outliers using z-score method
    
    Args:
        df: DataFrame to analyze
        column: Numeric column to check for outliers
        threshold: Z-score threshold (default: 3.0)
    
    Returns:
        DataFrame with outlier flag
    """
    # Calculate mean and standard deviation
    stats = df.select(
        F.mean(F.col(column)).alias('mean'),
        F.stddev(F.col(column)).alias('stddev')
    ).collect()[0]
    
    mean_val = stats['mean']
    stddev_val = stats['stddev']
    
    # Add z-score and outlier flag
    df_with_outliers = (
        df.withColumn(
            f'{column}_zscore',
            F.abs((F.col(column) - mean_val) / stddev_val)
        )
        .withColumn(
            f'{column}_is_outlier',
            F.when(F.col(f'{column}_zscore') > threshold, 1).otherwise(0)
        )
    )
    
    outlier_count = df_with_outliers.filter(F.col(f'{column}_is_outlier') == 1).count()
    total_count = df_with_outliers.count()
    
    logger.info(f"Outliers in '{column}': {outlier_count:,} ({outlier_count/total_count*100:.2f}%)")
    
    return df_with_outliers


def export_sample_to_local(df: DataFrame, output_path: str, sample_size: int = 10000) -> None:
    """
    Export a sample of data to local CSV for quick inspection
    
    Args:
        df: DataFrame to sample
        output_path: Local file path for output
        sample_size: Number of records to sample
    """
    logger.info(f"Exporting sample of {sample_size:,} records to {output_path}")
    
    sample_df = df.limit(sample_size).toPandas()
    sample_df.to_csv(output_path, index=False)
    
    logger.info(f"✅ Sample exported successfully")


def create_data_lineage_report(pipeline_stats: Dict) -> str:
    """
    Generate a data lineage report for documentation
    
    Args:
        pipeline_stats: Dictionary with pipeline execution statistics
    
    Returns:
        Markdown-formatted lineage report
    """
    report = f"""
# Data Lineage Report - Medicare Part D ETL Pipeline

**Generated**: {pipeline_stats.get('timestamp', 'N/A')}  
**Processing Year**: {pipeline_stats.get('year', 'N/A')}  
**Execution Mode**: {pipeline_stats.get('mode', 'N/A')}  

---

## Pipeline Execution Summary

| Metric | Value |
|--------|-------|
| Total Duration | {pipeline_stats.get('duration', 'N/A')} seconds |
| Records Extracted | {pipeline_stats.get('records_extracted', 'N/A'):,} |
| Records Cleaned | {pipeline_stats.get('records_cleaned', 'N/A'):,} |
| Data Loss | {pipeline_stats.get('data_loss_pct', 'N/A')}% |

---

## Data Quality Metrics

| Metric | Count | Percentage |
|--------|-------|------------|
| High-Risk Prescriptions | {pipeline_stats.get('high_risk_count', 'N/A'):,} | {pipeline_stats.get('high_risk_pct', 'N/A')}% |
| Opioid Prescriptions | {pipeline_stats.get('opioid_count', 'N/A'):,} | {pipeline_stats.get('opioid_pct', 'N/A')}% |
| Geographic Hotspots | {pipeline_stats.get('hotspot_count', 'N/A'):,} | - |

---

## Data Transformations Applied

1. **Extraction**: Raw data loaded from S3 data lake
2. **Cleaning**: Removed records below CMS privacy thresholds (claims < 11)
3. **Validation**: Filtered outliers with cost per claim > $50,000
4. **Enrichment**: Joined with NPI Registry for provider metadata
5. **Classification**: Flagged high-risk drugs (opioids, benzos, stimulants)
6. **Aggregation**: Calculated state, ZIP, and temporal aggregates
7. **Hotspot Detection**: Identified 95th percentile geographic concentrations

---

## Output Datasets

- Provider Summary: `processed/tableau_ready/provider_summary/`
- Drug Summary: `processed/tableau_ready/drug_summary/`
- State Hotspots: `processed/hotspots/state_level/`
- ZIP Hotspots: `processed/hotspots/zip_opioid/`
- Temporal Trends: `processed/temporal/yearly_trends/`

---

**Pipeline Version**: 1.0.0  
**Spark Version**: 3.x  
**Contact**: GitHub - @gordantao
"""
    
    return report


if __name__ == "__main__":
    # Test utilities with mock data
    from extract.extract_s3_data import get_spark_session
    
    spark = get_spark_session()
    
    # Create test DataFrame
    test_data = [
        (1, "Provider A", 100, 5000.0),
        (2, "Provider B", 200, 10000.0),
        (3, None, 150, 7500.0),  # Missing name
        (4, "Provider D", 50, 100000.0),  # Outlier cost
    ]
    
    test_df = spark.createDataFrame(test_data, ['id', 'name', 'claims', 'cost'])
    
    # Test utilities
    print_dataframe_summary(test_df, "Test DataFrame")
    validate_dataframe(test_df, ['id', 'name', 'claims', 'cost'])
    score = calculate_data_quality_score(test_df, ['id', 'name', 'claims'])
    
    print(f"\nData Quality Score: {score:.2f}%")
