"""
Load module for writing processed data to S3 for consumption by Tableau
Optimized for analytical workloads with partitioning and compression
"""
from pyspark.sql import DataFrame
import config


def write_to_s3(df: DataFrame, path: str, partition_by: list = None, 
                mode: str = 'overwrite', format: str = 'parquet') -> None:
    """
    Write DataFrame to S3 with optimal settings for analytics
    
    Args:
        df: DataFrame to write
        path: S3 destination path
        partition_by: List of columns to partition by
        mode: Write mode ('overwrite', 'append', 'error')
        format: Output format ('parquet', 'csv', 'delta')
    """
    print(f"💾 Writing data to {path}")
    
    writer = df.write.mode(mode)
    
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    
    if format == 'parquet':
        writer = writer.option('compression', 'snappy')
    elif format == 'csv':
        writer = writer.option('header', 'true')
    
    writer.format(format).save(path)
    
    record_count = df.count()
    print(f"✅ Successfully wrote {record_count:,} records to {path}")


def load_cleaned_data(df: DataFrame) -> None:
    """
    Save the main cleaned and enriched dataset
    
    Args:
        df: Cleaned provider-drug DataFrame
    """
    print("\n📦 Loading cleaned data to S3...")
    
    write_to_s3(
        df=df,
        path=config.PROCESSED_CLEAN,
        partition_by=['provider_state', 'risk_category'],
        format='parquet'
    )


def load_provider_summary(df: DataFrame) -> None:
    """
    Save provider-level summary for Tableau
    
    Args:
        df: Provider summary DataFrame
    """
    print("\n📦 Loading provider summary...")
    
    write_to_s3(
        df=df,
        path=f"{config.PROCESSED_TABLEAU}provider_summary/",
        partition_by=['provider_state'],
        format='parquet'
    )
    
    # Also save as CSV for easy viewing
    write_to_s3(
        df=df.coalesce(10),  # Reduce file count
        path=f"{config.PROCESSED_TABLEAU}provider_summary_csv/",
        format='csv'
    )


def load_drug_summary(df: DataFrame) -> None:
    """
    Save drug-level summary for Tableau
    
    Args:
        df: Drug summary DataFrame
    """
    print("\n📦 Loading drug summary...")
    
    write_to_s3(
        df=df,
        path=f"{config.PROCESSED_TABLEAU}drug_summary/",
        partition_by=['risk_category'],
        format='parquet'
    )


def load_state_hotspots(df: DataFrame) -> None:
    """
    Save state-level hotspot data for geographic visualization
    
    Args:
        df: State hotspots DataFrame
    """
    print("\n📦 Loading state hotspots...")
    
    write_to_s3(
        df=df,
        path=f"{config.PROCESSED_HOTSPOTS}state_level/",
        format='parquet'
    )
    
    # CSV for Tableau map integration
    write_to_s3(
        df=df.coalesce(1),
        path=f"{config.PROCESSED_HOTSPOTS}state_level_csv/",
        format='csv'
    )


def load_zip_hotspots(df: DataFrame) -> None:
    """
    Save ZIP-level opioid hotspot data
    
    Args:
        df: ZIP-level opioid hotspots DataFrame
    """
    print("\n📦 Loading ZIP-level opioid hotspots...")
    
    write_to_s3(
        df=df,
        path=f"{config.PROCESSED_HOTSPOTS}zip_opioid/",
        partition_by=['provider_state'],
        format='parquet'
    )


def load_temporal_trends(df: DataFrame) -> None:
    """
    Save temporal trend data for time-series analysis
    
    Args:
        df: Temporal trends DataFrame
    """
    print("\n📦 Loading temporal trends...")
    
    write_to_s3(
        df=df,
        path=f"{config.PROCESSED_TEMPORAL}yearly_trends/",
        partition_by=['report_year'],
        format='parquet'
    )


def load_specialty_risk(df: DataFrame) -> None:
    """
    Save specialty-level risk analysis
    
    Args:
        df: Specialty risk DataFrame
    """
    print("\n📦 Loading specialty risk analysis...")
    
    write_to_s3(
        df=df,
        path=f"{config.PROCESSED_TABLEAU}specialty_risk/",
        format='parquet'
    )


def load_tableau_hyper_extracts(datasets: dict) -> None:
    """
    Create optimized single-file extracts for Tableau
    Combines key datasets with appropriate aggregation
    
    Args:
        datasets: Dictionary of processed DataFrames
    """
    print("\n📊 Creating Tableau-optimized extracts...")
    
    # Create a unified dashboard dataset
    # This is a denormalized view combining key metrics
    from pyspark.sql import functions as F
    
    # Create comprehensive extract for main dashboard
    main_dashboard = (
        datasets.get('provider_summary')
        .select(
            'npi',
            'provider_name',
            'provider_city',
            'provider_state',
            'provider_zip',
            'specialty',
            'provider_total_claims',
            'provider_total_cost',
            'provider_opioid_claims',
            'provider_highrisk_claims',
            'opioid_percentage',
            'unique_drugs_prescribed',
            'total_beneficiaries'
        )
        .coalesce(5)  # Consolidate to fewer files for faster Tableau load
    )
    
    write_to_s3(
        df=main_dashboard,
        path=f"{config.PROCESSED_TABLEAU}main_dashboard/",
        format='csv'
    )
    
    print("✅ Tableau extracts created")


def load_all_datasets(transformed_datasets: dict) -> None:
    """
    Load all transformed datasets to S3
    
    Args:
        transformed_datasets: Dictionary of all processed DataFrames
    """
    print("\n" + "="*70)
    print("📤 Starting Data Load to S3")
    print("="*70 + "\n")
    
    # Load primary datasets
    load_cleaned_data(transformed_datasets['cleaned_data'])
    load_provider_summary(transformed_datasets['provider_summary'])
    load_drug_summary(transformed_datasets['drug_summary'])
    
    # Load geographic hotspots
    load_state_hotspots(transformed_datasets['state_aggregates'])
    load_zip_hotspots(transformed_datasets['zip_opioid_aggregates'])
    
    # Load temporal data
    load_temporal_trends(transformed_datasets['temporal_trends'])
    
    # Load specialty analysis
    load_specialty_risk(transformed_datasets['specialty_risk'])
    
    # Create Tableau-ready extracts
    load_tableau_hyper_extracts(transformed_datasets)
    
    print("\n" + "="*70)
    print("✅ Data Load Complete - All datasets available in S3")
    print("="*70)
    print(f"\n📊 Tableau Dashboard Data: {config.PROCESSED_TABLEAU}")
    print(f"🔥 Hotspot Analysis Data: {config.PROCESSED_HOTSPOTS}")
    print(f"📈 Temporal Trends Data: {config.PROCESSED_TEMPORAL}")
    print(f"🧹 Cleaned Raw Data: {config.PROCESSED_CLEAN}\n")


def create_data_catalog() -> dict:
    """
    Create a data catalog documenting all output datasets
    
    Returns:
        Dictionary with dataset metadata
    """
    catalog = {
        'datasets': [
            {
                'name': 'Provider Summary',
                'path': f"{config.PROCESSED_TABLEAU}provider_summary/",
                'format': 'parquet',
                'description': 'Provider-level aggregates with opioid metrics',
                'use_case': 'Provider drill-down analysis'
            },
            {
                'name': 'Drug Summary',
                'path': f"{config.PROCESSED_TABLEAU}drug_summary/",
                'format': 'parquet',
                'description': 'Drug-level aggregates by risk category',
                'use_case': 'Drug utilization analysis'
            },
            {
                'name': 'State Hotspots',
                'path': f"{config.PROCESSED_HOTSPOTS}state_level/",
                'format': 'parquet/csv',
                'description': 'State-level prescription hotspots',
                'use_case': 'Geographic heat maps'
            },
            {
                'name': 'ZIP Opioid Hotspots',
                'path': f"{config.PROCESSED_HOTSPOTS}zip_opioid/",
                'format': 'parquet',
                'description': 'ZIP-level opioid prescription concentrations',
                'use_case': 'Detailed geographic analysis'
            },
            {
                'name': 'Temporal Trends',
                'path': f"{config.PROCESSED_TEMPORAL}yearly_trends/",
                'format': 'parquet',
                'description': 'Year-over-year prescription trends',
                'use_case': 'Time-series analysis'
            },
            {
                'name': 'Specialty Risk',
                'path': f"{config.PROCESSED_TABLEAU}specialty_risk/",
                'format': 'parquet',
                'description': 'Provider specialty risk profiles',
                'use_case': 'Specialty-based analysis'
            },
            {
                'name': 'Main Dashboard',
                'path': f"{config.PROCESSED_TABLEAU}main_dashboard/",
                'format': 'csv',
                'description': 'Unified dataset for primary Tableau dashboard',
                'use_case': 'Primary visualization dashboard'
            }
        ]
    }
    
    return catalog


if __name__ == "__main__":
    from extract.extract_s3_data import get_spark_session, extract_all_datasets
    from transform.transform_partd import run_full_transformation
    
    # Test load pipeline
    spark = get_spark_session()
    raw_datasets = extract_all_datasets(spark, year="2023")
    transformed_datasets = run_full_transformation(raw_datasets)
    
    # Load to S3
    load_all_datasets(transformed_datasets)
    
    # Display catalog
    catalog = create_data_catalog()
    print("\n📚 Data Catalog:")
    for dataset in catalog['datasets']:
        print(f"\n  • {dataset['name']}")
        print(f"    Path: {dataset['path']}")
        print(f"    Use: {dataset['use_case']}")
