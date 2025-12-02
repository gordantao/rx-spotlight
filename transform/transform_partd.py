"""
Transformation module for cleaning, enriching, and analyzing Medicare Part D data
Handles 100M+ rows with optimized Spark operations
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import config


def standardize_column_names(df: DataFrame, column_mapping: dict) -> DataFrame:
    """
    Standardize column names based on mapping
    """
    for old_name, new_name in column_mapping.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)
    
    return df


def clean_provider_drug_data(df: DataFrame) -> DataFrame:
    """
    Clean and validate the main provider-drug dataset
    
    Args:
        df: Raw provider-drug DataFrame
    
    Returns:
        Cleaned DataFrame
    """
    print("🧹 Cleaning provider-drug data...")
    
    # Standardize column names
    df = standardize_column_names(df, config.PARTD_PROVIDER_DRUG_SCHEMA)
    
    # Data quality filters
    df_clean = (
        df
        # Remove records with missing critical fields
        .filter(F.col('npi').isNotNull())
        .filter(F.col('drug_name').isNotNull())
        .filter(F.col('total_claims').isNotNull())
        
        # Apply CMS privacy thresholds
        .filter(F.col('total_claims') >= config.MIN_CLAIM_COUNT)
        .filter(F.col('beneficiary_count') >= config.MIN_BENEFICIARIES)
        
        # Remove outliers and suspicious data
        .filter(
            (F.col('total_cost') / F.col('total_claims')) <= config.MAX_COST_PER_CLAIM
        )
        
        # Clean and standardize text fields
        .withColumn('drug_name', F.upper(F.trim(F.col('drug_name'))))
        .withColumn('generic_name', F.upper(F.trim(F.col('generic_name'))))
        .withColumn('provider_state', F.upper(F.trim(F.col('provider_state'))))
        .withColumn('specialty', F.trim(F.col('specialty')))
        
        # Add derived metrics
        .withColumn('cost_per_claim', F.col('total_cost') / F.col('total_claims'))
        .withColumn('cost_per_day', F.col('total_cost') / F.col('total_day_supply'))
        .withColumn('avg_day_supply', F.col('total_day_supply') / F.col('total_fills'))
        
        # Add data quality flag
        .withColumn('data_quality', F.lit('CLEAN'))
    )
    
    initial_count = df.count()
    clean_count = df_clean.count()
    removed = initial_count - clean_count
    
    print(f"✅ Cleaned data: {initial_count:,} → {clean_count:,} records")
    print(f"   Removed {removed:,} records ({removed/initial_count*100:.2f}%)")
    
    return df_clean


def flag_high_risk_drugs(df: DataFrame) -> DataFrame:
    """
    Identify and flag high-risk prescriptions (opioids, benzos, stimulants)
    
    Args:
        df: Cleaned provider-drug DataFrame
    
    Returns:
        DataFrame with risk flags
    """
    print("🚨 Flagging high-risk prescriptions...")
    
    # Create regex patterns for efficient matching
    opioid_pattern = '|'.join(config.OPIOID_KEYWORDS)
    benzo_pattern = '|'.join(config.BENZODIAZEPINE_KEYWORDS)
    stimulant_pattern = '|'.join(config.STIMULANT_KEYWORDS)
    
    df_flagged = (
        df
        .withColumn(
            'is_opioid',
            F.when(
                F.col('drug_name').rlike(opioid_pattern) |
                F.col('generic_name').rlike(opioid_pattern),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            'is_benzodiazepine',
            F.when(
                F.col('drug_name').rlike(benzo_pattern) |
                F.col('generic_name').rlike(benzo_pattern),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            'is_stimulant',
            F.when(
                F.col('drug_name').rlike(stimulant_pattern) |
                F.col('generic_name').rlike(stimulant_pattern),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            'is_high_risk',
            F.when(
                (F.col('is_opioid') == 1) |
                (F.col('is_benzodiazepine') == 1) |
                (F.col('is_stimulant') == 1),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        .withColumn(
            'risk_category',
            F.when(F.col('is_opioid') == 1, F.lit('OPIOID'))
            .when(F.col('is_benzodiazepine') == 1, F.lit('BENZODIAZEPINE'))
            .when(F.col('is_stimulant') == 1, F.lit('STIMULANT'))
            .otherwise(F.lit('STANDARD'))
        )
    )
    
    # Summary statistics
    total_records = df_flagged.count()
    high_risk_count = df_flagged.filter(F.col('is_high_risk') == 1).count()
    opioid_count = df_flagged.filter(F.col('is_opioid') == 1).count()
    
    print(f"✅ Risk Analysis Complete:")
    print(f"   Total prescriptions: {total_records:,}")
    print(f"   High-risk prescriptions: {high_risk_count:,} ({high_risk_count/total_records*100:.2f}%)")
    print(f"   Opioid prescriptions: {opioid_count:,} ({opioid_count/total_records*100:.2f}%)")
    
    return df_flagged


def enrich_with_provider_data(df: DataFrame, npi_df: DataFrame) -> DataFrame:
    """
    Join prescription data with NPI registry for enhanced provider information
    
    Args:
        df: Provider-drug DataFrame
        npi_df: NPI Registry DataFrame
    
    Returns:
        Enriched DataFrame
    """
    print("🔗 Enriching with NPI provider data...")
    
    # Select relevant NPI fields and standardize
    npi_clean = (
        npi_df
        .select(
            F.col('NPI').alias('npi'),
            F.col('Provider Organization Name (Legal Business Name)').alias('org_name'),
            F.col('Healthcare Provider Taxonomy Code_1').alias('taxonomy_code'),
            F.col('Provider Business Practice Location Address State Name').alias('npi_state'),
            F.col('Provider Business Practice Location Address Postal Code').alias('npi_zip')
        )
        .dropDuplicates(['npi'])
    )
    
    # Left join to preserve all prescription records
    df_enriched = (
        df.join(
            npi_clean,
            on='npi',
            how='left'
        )
    )
    
    print(f"✅ Provider data enrichment complete")
    
    return df_enriched


def add_temporal_dimensions(df: DataFrame, date_column: str = None) -> DataFrame:
    """
    Add temporal analysis columns (year, quarter, month)
    
    Args:
        df: DataFrame with date information
        date_column: Name of date column (if available)
    
    Returns:
        DataFrame with temporal dimensions
    """
    print("📅 Adding temporal dimensions...")
    
    # If no date column, use current date as placeholder
    # In real implementation, extract from filename or metadata
    if date_column and date_column in df.columns:
        df = df.withColumn('report_date', F.col(date_column))
    else:
        # Assume filename contains year information
        df = df.withColumn('report_year', F.lit(2023))
        df = df.withColumn('report_quarter', F.lit('Q4'))
    
    return df


def calculate_geographic_aggregates(df: DataFrame) -> DataFrame:
    """
    Calculate state-level and ZIP-level prescription aggregates
    
    Args:
        df: Clean provider-drug DataFrame with risk flags
    
    Returns:
        DataFrame with geographic aggregates
    """
    print("🗺️ Calculating geographic aggregates...")
    
    # State-level aggregates
    state_agg = (
        df.groupBy('provider_state', 'risk_category')
        .agg(
            F.sum('total_claims').alias('state_total_claims'),
            F.sum('total_cost').alias('state_total_cost'),
            F.countDistinct('npi').alias('state_provider_count'),
            F.sum('beneficiary_count').alias('state_beneficiary_count'),
            F.avg('cost_per_claim').alias('state_avg_cost_per_claim')
        )
    )
    
    # ZIP-level aggregates for opioids specifically
    zip_opioid_agg = (
        df.filter(F.col('is_opioid') == 1)
        .groupBy('provider_zip', 'provider_state')
        .agg(
            F.sum('total_claims').alias('zip_opioid_claims'),
            F.sum('total_cost').alias('zip_opioid_cost'),
            F.countDistinct('npi').alias('zip_opioid_providers'),
            F.sum('beneficiary_count').alias('zip_opioid_beneficiaries')
        )
    )
    
    print(f"✅ Geographic aggregates calculated")
    
    return {
        'state_aggregates': state_agg,
        'zip_opioid_aggregates': zip_opioid_agg
    }


def identify_prescription_hotspots(df: DataFrame, percentile: float = 0.95) -> DataFrame:
    """
    Identify geographic hotspots with unusually high prescription volumes
    
    Args:
        df: DataFrame with geographic aggregates
        percentile: Percentile threshold for hotspot identification (default 95th)
    
    Returns:
        DataFrame with hotspot flags
    """
    print(f"🔥 Identifying prescription hotspots (>{percentile*100}th percentile)...")
    
    # Calculate percentile thresholds for high-risk prescriptions
    df_with_hotspot = (
        df
        # Add per-capita metrics if population data available
        .withColumn('claims_per_beneficiary', 
                   F.col('total_claims') / F.col('beneficiary_count'))
        
        # Calculate state-level rankings
        .withColumn(
            'state_opioid_rank',
            F.percent_rank().over(
                Window.partitionBy('risk_category')
                .orderBy(F.col('total_claims').desc())
            )
        )
        
        # Flag hotspots
        .withColumn(
            'is_hotspot',
            F.when(F.col('state_opioid_rank') >= percentile, F.lit(1))
            .otherwise(F.lit(0))
        )
    )
    
    hotspot_count = df_with_hotspot.filter(F.col('is_hotspot') == 1).count()
    print(f"✅ Identified {hotspot_count} geographic hotspots")
    
    return df_with_hotspot


def calculate_temporal_trends(df: DataFrame) -> DataFrame:
    """
    Calculate temporal trends for time-series analysis
    Note: Requires multiple years of data for true trend analysis
    
    Args:
        df: Provider-drug DataFrame with temporal dimensions
    
    Returns:
        DataFrame with temporal aggregates
    """
    print("📈 Calculating temporal trends...")
    
    temporal_agg = (
        df.groupBy('report_year', 'provider_state', 'risk_category')
        .agg(
            F.sum('total_claims').alias('yearly_claims'),
            F.sum('total_cost').alias('yearly_cost'),
            F.countDistinct('npi').alias('yearly_providers'),
            F.avg('cost_per_claim').alias('avg_cost_per_claim')
        )
        .orderBy('report_year', 'provider_state', 'risk_category')
    )
    
    print(f"✅ Temporal trends calculated")
    
    return temporal_agg


def create_tableau_ready_dataset(df: DataFrame, geo_agg: dict) -> dict:
    """
    Create optimized, denormalized datasets ready for Tableau visualization
    
    Args:
        df: Main enriched DataFrame
        geo_agg: Dictionary of geographic aggregates
    
    Returns:
        Dictionary of Tableau-ready DataFrames
    """
    print("📊 Preparing Tableau-ready datasets...")
    
    # 1. Provider-level summary for drill-down analysis
    provider_summary = (
        df.groupBy(
            'npi', 'provider_name', 'provider_city', 
            'provider_state', 'provider_zip', 'specialty'
        )
        .agg(
            F.sum('total_claims').alias('provider_total_claims'),
            F.sum('total_cost').alias('provider_total_cost'),
            F.sum(F.when(F.col('is_opioid') == 1, F.col('total_claims'))).alias('provider_opioid_claims'),
            F.sum(F.when(F.col('is_high_risk') == 1, F.col('total_claims'))).alias('provider_highrisk_claims'),
            F.countDistinct('drug_name').alias('unique_drugs_prescribed'),
            F.sum('beneficiary_count').alias('total_beneficiaries')
        )
        .withColumn('opioid_percentage', 
                   F.col('provider_opioid_claims') / F.col('provider_total_claims') * 100)
    )
    
    # 2. Drug-level analysis
    drug_summary = (
        df.groupBy('drug_name', 'generic_name', 'risk_category')
        .agg(
            F.sum('total_claims').alias('drug_total_claims'),
            F.sum('total_cost').alias('drug_total_cost'),
            F.countDistinct('npi').alias('prescriber_count'),
            F.avg('cost_per_claim').alias('avg_cost_per_claim'),
            F.sum('beneficiary_count').alias('total_patients')
        )
        .orderBy(F.col('drug_total_claims').desc())
    )
    
    # 3. State-level hotspot map data
    state_hotspots = geo_agg['state_aggregates']
    
    # 4. ZIP-level opioid hotspots
    zip_hotspots = geo_agg['zip_opioid_aggregates']
    
    # 5. Specialty analysis
    specialty_risk = (
        df.groupBy('specialty', 'risk_category')
        .agg(
            F.sum('total_claims').alias('specialty_claims'),
            F.sum('total_cost').alias('specialty_cost'),
            F.countDistinct('npi').alias('specialty_provider_count')
        )
        .orderBy(F.col('specialty_claims').desc())
    )
    
    print(f"✅ Created 5 Tableau-ready datasets")
    
    return {
        'provider_summary': provider_summary,
        'drug_summary': drug_summary,
        'state_hotspots': state_hotspots,
        'zip_hotspots': zip_hotspots,
        'specialty_risk': specialty_risk
    }


def run_full_transformation(datasets: dict) -> dict:
    """
    Execute the complete transformation pipeline
    
    Args:
        datasets: Dictionary of raw DataFrames from extraction
    
    Returns:
        Dictionary of transformed DataFrames
    """
    print("\n" + "="*70)
    print("🔄 Starting Data Transformation Pipeline")
    print("="*70 + "\n")
    
    # Step 1: Clean main dataset
    df_clean = clean_provider_drug_data(datasets['provider_drug'])
    
    # Step 2: Flag high-risk drugs
    df_flagged = flag_high_risk_drugs(df_clean)
    
    # Step 3: Enrich with NPI data
    df_enriched = enrich_with_provider_data(df_flagged, datasets['npi_registry'])
    
    # Step 4: Add temporal dimensions
    df_temporal = add_temporal_dimensions(df_enriched)
    
    # Step 5: Calculate geographic aggregates
    geo_aggregates = calculate_geographic_aggregates(df_temporal)
    
    # Step 6: Identify hotspots
    state_hotspots = identify_prescription_hotspots(
        geo_aggregates['state_aggregates']
    )
    geo_aggregates['state_aggregates'] = state_hotspots
    
    # Step 7: Calculate temporal trends
    temporal_trends = calculate_temporal_trends(df_temporal)
    
    # Step 8: Create Tableau-ready datasets
    tableau_datasets = create_tableau_ready_dataset(df_temporal, geo_aggregates)
    
    print("\n" + "="*70)
    print("✅ Transformation Pipeline Complete")
    print("="*70 + "\n")
    
    return {
        'cleaned_data': df_temporal,
        'temporal_trends': temporal_trends,
        **tableau_datasets,
        **geo_aggregates
    }


if __name__ == "__main__":
    from extract.extract_s3_data import get_spark_session, extract_all_datasets
    
    # Test transformation pipeline
    spark = get_spark_session()
    raw_datasets = extract_all_datasets(spark, year="2023")
    transformed_datasets = run_full_transformation(raw_datasets)
    
    print("\n📊 Sample Transformed Data - Provider Summary:")
    transformed_datasets['provider_summary'].show(10)
    
    print("\n📊 Sample Opioid Hotspots by State:")
    transformed_datasets['state_aggregates'].filter(
        F.col('risk_category') == 'OPIOID'
    ).orderBy(F.col('state_total_claims').desc()).show(10)
