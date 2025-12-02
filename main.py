"""
Medicare Part D ETL Pipeline - Main Orchestrator
Processes 100M+ rows of prescription data for public health insights

Usage:
    python main.py --year 2023 --mode full
    python main.py --year 2023 --mode extract
    python main.py --year 2023 --mode transform
    python main.py --year 2023 --mode load
"""
import argparse
import time
from datetime import datetime
import sys

from extract.extract_s3_data import get_spark_session, extract_all_datasets
from transform.transform_partd import run_full_transformation
from load.load_to_s3 import load_all_datasets, create_data_catalog
import config


def print_banner():
    """Print pipeline banner"""
    banner = """
    ╔══════════════════════════════════════════════════════════════════════╗
    ║                                                                      ║
    ║           Medicare Part D Prescription Analytics Pipeline           ║
    ║                                                                      ║
    ║     Analyzing 100M+ Prescriptions for Public Health Insights        ║
    ║                                                                      ║
    ╚══════════════════════════════════════════════════════════════════════╝
    """
    print(banner)


def print_summary(stats: dict):
    """
    Print pipeline execution summary
    
    Args:
        stats: Dictionary with pipeline statistics
    """
    print("\n" + "="*70)
    print("📊 PIPELINE EXECUTION SUMMARY")
    print("="*70)
    print(f"\n⏱️  Total Duration: {stats['duration']:.2f} seconds")
    print(f"📅 Processing Date: {stats['timestamp']}")
    print(f"📁 Year: {stats['year']}")
    print(f"\n📈 Data Processing Statistics:")
    print(f"   • Records Extracted: {stats.get('records_extracted', 'N/A')}")
    print(f"   • Records Cleaned: {stats.get('records_cleaned', 'N/A')}")
    print(f"   • High-Risk Prescriptions: {stats.get('high_risk_count', 'N/A')}")
    print(f"   • Opioid Prescriptions: {stats.get('opioid_count', 'N/A')}")
    print(f"   • Geographic Hotspots: {stats.get('hotspot_count', 'N/A')}")
    print(f"\n📊 Output Datasets:")
    print(f"   • Provider Summary: {config.PROCESSED_TABLEAU}provider_summary/")
    print(f"   • Drug Summary: {config.PROCESSED_TABLEAU}drug_summary/")
    print(f"   • State Hotspots: {config.PROCESSED_HOTSPOTS}state_level/")
    print(f"   • ZIP Hotspots: {config.PROCESSED_HOTSPOTS}zip_opioid/")
    print(f"   • Temporal Trends: {config.PROCESSED_TEMPORAL}yearly_trends/")
    print(f"   • Main Dashboard: {config.PROCESSED_TABLEAU}main_dashboard/")
    print("\n" + "="*70)
    print("✅ Pipeline execution completed successfully!")
    print("="*70 + "\n")


def run_pipeline(year: str = "2023", mode: str = "full"):
    """
    Run the complete ETL pipeline
    
    Args:
        year: Year to process (e.g., '2023')
        mode: Pipeline mode ('full', 'extract', 'transform', 'load')
    
    Returns:
        Dictionary with execution statistics
    """
    start_time = time.time()
    stats = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'year': year,
        'mode': mode
    }
    
    print_banner()
    
    # Initialize Spark
    print("🚀 Initializing Spark session...")
    spark = get_spark_session()
    print(f"✅ Spark session initialized: {spark.sparkContext.appName}")
    print(f"   Spark version: {spark.version}")
    print(f"   Master: {spark.sparkContext.master}\n")
    
    try:
        # EXTRACT Phase
        if mode in ['full', 'extract']:
            print("="*70)
            print("PHASE 1: EXTRACT")
            print("="*70 + "\n")
            raw_datasets = extract_all_datasets(spark, year=year)
            
            # Cache for reuse
            raw_datasets['provider_drug'].cache()
            stats['records_extracted'] = raw_datasets['provider_drug'].count()
        
        # TRANSFORM Phase
        if mode in ['full', 'transform']:
            print("\n" + "="*70)
            print("PHASE 2: TRANSFORM")
            print("="*70 + "\n")
            
            if mode == 'transform':
                # Load from processed if running transform only
                raw_datasets = extract_all_datasets(spark, year=year)
            
            transformed_datasets = run_full_transformation(raw_datasets)
            
            # Collect statistics
            if 'cleaned_data' in transformed_datasets:
                stats['records_cleaned'] = transformed_datasets['cleaned_data'].count()
                stats['high_risk_count'] = (
                    transformed_datasets['cleaned_data']
                    .filter("is_high_risk = 1")
                    .count()
                )
                stats['opioid_count'] = (
                    transformed_datasets['cleaned_data']
                    .filter("is_opioid = 1")
                    .count()
                )
        
        # LOAD Phase
        if mode in ['full', 'load']:
            print("\n" + "="*70)
            print("PHASE 3: LOAD")
            print("="*70 + "\n")
            
            if mode == 'load':
                # Load from processed if running load only
                raw_datasets = extract_all_datasets(spark, year=year)
                transformed_datasets = run_full_transformation(raw_datasets)
            
            load_all_datasets(transformed_datasets)
            
            # Create data catalog
            catalog = create_data_catalog()
            stats['datasets_created'] = len(catalog['datasets'])
        
        # Calculate execution time
        end_time = time.time()
        stats['duration'] = end_time - start_time
        
        # Print summary
        print_summary(stats)
        
        return stats
        
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        # Cleanup
        print("🧹 Cleaning up Spark session...")
        spark.stop()
        print("✅ Spark session stopped\n")


def main():
    """Main entry point with argument parsing"""
    parser = argparse.ArgumentParser(
        description='Medicare Part D ETL Pipeline - Process prescription data for public health insights'
    )
    
    parser.add_argument(
        '--year',
        type=str,
        default='2023',
        help='Year to process (default: 2023)'
    )
    
    parser.add_argument(
        '--mode',
        type=str,
        choices=['full', 'extract', 'transform', 'load'],
        default='full',
        help='Pipeline execution mode (default: full)'
    )
    
    parser.add_argument(
        '--profile',
        action='store_true',
        help='Enable profiling for performance analysis'
    )
    
    args = parser.parse_args()
    
    # Run pipeline
    if args.profile:
        import cProfile
        import pstats
        
        profiler = cProfile.Profile()
        profiler.enable()
        
        stats = run_pipeline(year=args.year, mode=args.mode)
        
        profiler.disable()
        stats_obj = pstats.Stats(profiler)
        stats_obj.sort_stats('cumulative')
        stats_obj.print_stats(20)
    else:
        stats = run_pipeline(year=args.year, mode=args.mode)
    
    return stats


if __name__ == "__main__":
    main()
