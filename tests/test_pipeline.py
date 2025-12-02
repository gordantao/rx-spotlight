"""
Unit tests for Medicare Part D ETL Pipeline
Run with: pytest tests/test_pipeline.py -v
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    spark = SparkSession.builder \
        .appName("PartD-Test") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture
def sample_prescription_data(spark):
    """Create sample prescription data for testing"""
    data = [
        (12345, "Dr. Smith", "New York", "NY", "10001", "Internal Medicine", 
         "OXYCODONE HCL", "OXYCODONE", 100, 50, 1000, 5000.0, 25),
        (23456, "Dr. Jones", "Los Angeles", "CA", "90001", "Pain Management",
         "HYDROCODONE-ACETAMINOPHEN", "HYDROCODONE", 200, 100, 2000, 10000.0, 50),
        (34567, "Dr. Williams", "Chicago", "IL", "60601", "Family Practice",
         "LISINOPRIL", "LISINOPRIL", 150, 75, 1500, 3000.0, 40),
        (45678, "Dr. Brown", "Houston", "TX", "77001", "Cardiology",
         "ATORVASTATIN", "ATORVASTATIN", 300, 150, 3000, 8000.0, 80),
    ]
    
    columns = [
        'npi', 'provider_name', 'provider_city', 'provider_state', 'provider_zip',
        'specialty', 'drug_name', 'generic_name', 'total_claims', 'total_fills',
        'total_day_supply', 'total_cost', 'beneficiary_count'
    ]
    
    return spark.createDataFrame(data, columns)


class TestDataCleaning:
    """Test data cleaning functions"""
    
    def test_remove_low_claim_records(self, spark, sample_prescription_data):
        """Test that records with claims < 11 are removed"""
        # Add a low-claim record
        low_claim_data = [(99999, "Dr. Test", "Boston", "MA", "02101", "Test",
                          "TEST_DRUG", "TEST", 5, 2, 50, 100.0, 3)]
        
        low_claim_df = spark.createDataFrame(
            low_claim_data,
            sample_prescription_data.columns
        )
        
        combined_df = sample_prescription_data.union(low_claim_df)
        
        # Apply filter (MIN_CLAIM_COUNT = 11)
        cleaned_df = combined_df.filter(F.col('total_claims') >= 11)
        
        assert cleaned_df.count() == 4, "Should remove low-claim records"
        assert cleaned_df.filter(F.col('npi') == 99999).count() == 0
    
    def test_standardize_drug_names(self, sample_prescription_data):
        """Test that drug names are uppercased and trimmed"""
        df = sample_prescription_data.withColumn(
            'drug_name',
            F.upper(F.trim(F.col('drug_name')))
        )
        
        drug_names = [row.drug_name for row in df.collect()]
        
        assert all(name.isupper() for name in drug_names)
        assert "OXYCODONE HCL" in drug_names


class TestOpioidDetection:
    """Test opioid detection logic"""
    
    def test_opioid_flag(self, sample_prescription_data):
        """Test that opioid drugs are correctly flagged"""
        opioid_pattern = 'OXYCODONE|HYDROCODONE|FENTANYL|MORPHINE'
        
        df = sample_prescription_data.withColumn(
            'is_opioid',
            F.when(
                F.col('drug_name').rlike(opioid_pattern),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        
        opioid_count = df.filter(F.col('is_opioid') == 1).count()
        assert opioid_count == 2, "Should identify 2 opioid prescriptions"
    
    def test_risk_category(self, sample_prescription_data):
        """Test risk category assignment"""
        opioid_pattern = 'OXYCODONE|HYDROCODONE'
        
        df = sample_prescription_data.withColumn(
            'is_opioid',
            F.when(F.col('drug_name').rlike(opioid_pattern), F.lit(1))
            .otherwise(F.lit(0))
        ).withColumn(
            'risk_category',
            F.when(F.col('is_opioid') == 1, F.lit('OPIOID'))
            .otherwise(F.lit('STANDARD'))
        )
        
        opioid_records = df.filter(F.col('risk_category') == 'OPIOID').count()
        assert opioid_records == 2


class TestGeographicAggregation:
    """Test geographic aggregation functions"""
    
    def test_state_aggregation(self, sample_prescription_data):
        """Test state-level aggregation"""
        state_agg = (
            sample_prescription_data
            .groupBy('provider_state')
            .agg(
                F.sum('total_claims').alias('state_total_claims'),
                F.countDistinct('npi').alias('state_provider_count')
            )
        )
        
        assert state_agg.count() == 4, "Should have 4 states"
        
        # Check NY has correct values
        ny_data = state_agg.filter(F.col('provider_state') == 'NY').collect()[0]
        assert ny_data.state_total_claims == 100
        assert ny_data.state_provider_count == 1
    
    def test_zip_aggregation(self, sample_prescription_data):
        """Test ZIP-level aggregation"""
        zip_agg = (
            sample_prescription_data
            .groupBy('provider_zip', 'provider_state')
            .agg(
                F.sum('total_claims').alias('zip_total_claims')
            )
        )
        
        assert zip_agg.count() == 4, "Should have 4 unique ZIP codes"


class TestDerivedMetrics:
    """Test calculated metrics"""
    
    def test_cost_per_claim(self, sample_prescription_data):
        """Test cost per claim calculation"""
        df = sample_prescription_data.withColumn(
            'cost_per_claim',
            F.col('total_cost') / F.col('total_claims')
        )
        
        # Check Dr. Smith's cost per claim
        smith_cost = df.filter(F.col('npi') == 12345).collect()[0].cost_per_claim
        assert smith_cost == 50.0, "Cost per claim should be 5000/100 = 50"
    
    def test_avg_day_supply(self, sample_prescription_data):
        """Test average day supply calculation"""
        df = sample_prescription_data.withColumn(
            'avg_day_supply',
            F.col('total_day_supply') / F.col('total_fills')
        )
        
        # Check Dr. Smith's avg day supply
        smith_days = df.filter(F.col('npi') == 12345).collect()[0].avg_day_supply
        assert smith_days == 20.0, "Avg day supply should be 1000/50 = 20"


class TestDataQuality:
    """Test data quality checks"""
    
    def test_no_null_npi(self, sample_prescription_data):
        """Test that NPI is never null"""
        null_count = sample_prescription_data.filter(
            F.col('npi').isNull()
        ).count()
        
        assert null_count == 0, "NPI should never be null"
    
    def test_positive_claims(self, sample_prescription_data):
        """Test that claims are always positive"""
        negative_claims = sample_prescription_data.filter(
            F.col('total_claims') <= 0
        ).count()
        
        assert negative_claims == 0, "Claims should always be positive"
    
    def test_cost_validation(self, sample_prescription_data):
        """Test that cost per claim is reasonable"""
        MAX_COST_PER_CLAIM = 50000
        
        df = sample_prescription_data.withColumn(
            'cost_per_claim',
            F.col('total_cost') / F.col('total_claims')
        )
        
        outliers = df.filter(
            F.col('cost_per_claim') > MAX_COST_PER_CLAIM
        ).count()
        
        assert outliers == 0, "No outlier costs in test data"


class TestProviderAnalysis:
    """Test provider-level analysis"""
    
    def test_provider_summary(self, sample_prescription_data):
        """Test provider-level aggregation"""
        provider_summary = (
            sample_prescription_data
            .groupBy('npi', 'provider_name', 'specialty')
            .agg(
                F.sum('total_claims').alias('provider_total_claims'),
                F.sum('total_cost').alias('provider_total_cost'),
                F.countDistinct('drug_name').alias('unique_drugs')
            )
        )
        
        assert provider_summary.count() == 4, "Should have 4 providers"
        
        # Each provider prescribed 1 unique drug in test data
        unique_drug_counts = [
            row.unique_drugs for row in provider_summary.collect()
        ]
        assert all(count == 1 for count in unique_drug_counts)


class TestHotspotDetection:
    """Test hotspot identification logic"""
    
    def test_percentile_ranking(self, spark):
        """Test percentile-based hotspot detection"""
        from pyspark.sql.window import Window
        
        data = [(i, i*100) for i in range(1, 21)]  # 20 records
        df = spark.createDataFrame(data, ['id', 'claims'])
        
        df_ranked = df.withColumn(
            'percentile_rank',
            F.percent_rank().over(Window.orderBy(F.col('claims').desc()))
        ).withColumn(
            'is_hotspot',
            F.when(F.col('percentile_rank') >= 0.95, F.lit(1)).otherwise(F.lit(0))
        )
        
        hotspot_count = df_ranked.filter(F.col('is_hotspot') == 1).count()
        
        # Top 5% of 20 records = 1 record
        assert hotspot_count == 1, "Should identify top 5% as hotspot"


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v", "--tb=short"])
