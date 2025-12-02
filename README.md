# RX Spotlight - Medicare Part D Prescription Analytics

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![PySpark](https://img.shields.io/badge/PySpark-3.x-orange.svg)](https://spark.apache.org/)
[![AWS S3](https://img.shields.io/badge/AWS-S3-yellow.svg)](https://aws.amazon.com/s3/)

## Project Overview

**RX Spotlight** is a comprehensive PySpark ETL pipeline designed to process, analyze, and visualize over **100 million rows** of Medicare Part D prescription data from an S3 data lake. The pipeline identifies geographic and temporal patterns in high-risk prescriptions (particularly opioids) to support public health insights and policy decisions.

### Key Features

- ⚡ **Scalable Processing**: Handles 100M+ prescription records using distributed PySpark processing
- 🔍 **High-Risk Detection**: Automatically identifies opioid, benzodiazepine, and stimulant prescriptions
- 🗺️ **Geographic Analysis**: Pinpoints prescription "hotspots" at state and ZIP code levels
- 📈 **Temporal Trends**: Tracks prescription patterns over time for trend analysis
- 📊 **Tableau Ready**: Generates optimized datasets for interactive dashboard visualization
- 🏥 **Provider Insights**: Analyzes prescriber patterns by specialty and location

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        S3 Data Lake                             │
│  raw/partd/provider_drug/ (100M+ rows)                         │
│  raw/npi/ (Provider Registry)                                   │
│  raw/census/ (Population Data)                                  │
│  raw/drug_reference/ (Opioid Classifications)                   │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                    PySpark ETL Pipeline                         │
│                                                                  │
│  1. EXTRACT  → Read from S3 with schema validation              │
│  2. TRANSFORM → Clean, join, flag high-risk drugs               │
│  3. ANALYZE  → Geographic hotspots & temporal trends            │
│  4. LOAD     → Write to S3 (Parquet/CSV)                        │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                   Processed Data (S3)                           │
│  processed/tableau_ready/                                       │
│  processed/hotspots/                                            │
│  processed/temporal/                                            │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│              Tableau Dashboard (Interactive)                    │
│  • Provider drill-down analysis                                 │
│  • Geographic heat maps (state/ZIP)                             │
│  • Opioid prescription trends                                   │
│  • Specialty risk profiles                                      │
└─────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
rx-spotlight/
│
├── config.py                 # Configuration (S3 paths, Spark settings, drug lists)
├── main.py                   # Main ETL orchestrator
├── load_partd.py            # Legacy loader (replaced by modular pipeline)
│
├── extract/
│   └── extract_s3_data.py   # S3 data extraction with schema handling
│
├── transform/
│   └── transform_partd.py   # Data cleaning, joins, risk detection, analysis
│
├── load/
│   └── load_to_s3.py        # Write processed data to S3 (Parquet/CSV)
│
└── README.md                # This file
```

---

## Getting Started

### Prerequisites

- **Python 3.8+**
- **Apache Spark 3.x** (PySpark)
- **AWS Account** with S3 access
- **AWS CLI** configured with credentials

### Installation

```bash
# Clone repository
git clone https://github.com/gordantao/rx-spotlight.git
cd rx-spotlight

# Create virtual environment
conda create -n ontology python=3.8
conda activate ontology

# Install dependencies
pip install pyspark pandas boto3 pyarrow

# Configure AWS credentials
aws configure
```

### Configuration

Edit `config.py` to set your S3 bucket and paths:

```python
S3_BUCKET = "s3://your-medicare-bucket"
```

---

## Usage

### Run Full Pipeline

Process all data from extraction to loading:

```bash
python main.py --year 2023 --mode full
```

### Run Individual Phases

```bash
# Extract only
python main.py --year 2023 --mode extract

# Transform only
python main.py --year 2023 --mode transform

# Load only
python main.py --year 2023 --mode load
```

### With Performance Profiling

```bash
python main.py --year 2023 --mode full --profile
```

---

## Output Datasets

The pipeline generates the following datasets for Tableau:

| Dataset | Path | Description | Use Case |
|---------|------|-------------|----------|
| **Provider Summary** | `processed/tableau_ready/provider_summary/` | Provider-level aggregates with opioid metrics | Provider drill-down analysis |
| **Drug Summary** | `processed/tableau_ready/drug_summary/` | Drug-level aggregates by risk category | Drug utilization analysis |
| **State Hotspots** | `processed/hotspots/state_level/` | State-level prescription concentrations | Geographic heat maps |
| **ZIP Hotspots** | `processed/hotspots/zip_opioid/` | ZIP-level opioid prescriptions | Detailed geographic analysis |
| **Temporal Trends** | `processed/temporal/yearly_trends/` | Year-over-year trends | Time-series analysis |
| **Specialty Risk** | `processed/tableau_ready/specialty_risk/` | Provider specialty risk profiles | Specialty-based analysis |
| **Main Dashboard** | `processed/tableau_ready/main_dashboard/` | Unified dataset (CSV) | Primary Tableau dashboard |

---

## Key Analysis Features

### High-Risk Drug Detection

The pipeline automatically flags prescriptions for:
- **Opioids**: Oxycodone, Hydrocodone, Fentanyl, Morphine, etc.
- **Benzodiazepines**: Xanax, Valium, Ativan, etc.
- **Stimulants**: Adderall, Ritalin, Vyvanse, etc.

### Geographic Hotspot Identification

- State-level aggregation with risk categorization
- ZIP-code level analysis for opioid prescriptions
- Percentile-based hotspot flagging (95th percentile)
- Per-capita normalization using Census data

### Data Quality Controls

- Removes records with claims < 11 (CMS privacy threshold)
- Flags outliers with cost per claim > $50,000
- Validates NPI and drug name integrity
- Standardizes text fields (uppercase, trimmed)

---

## Performance Optimization

### Spark Configuration

The pipeline uses optimized Spark settings for large-scale processing:

```python
SPARK_CONFIG = {
    "spark.executor.memory": "8g",
    "spark.driver.memory": "4g",
    "spark.executor.cores": "4",
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.enabled": "true"
}
```

### Data Partitioning

- **Parquet format** with Snappy compression
- **Partitioned by**: `provider_state`, `risk_category`
- **Coalesced files** for Tableau (reduces small file overhead)

---

## Tableau Dashboard Integration

### Connecting to Data

1. **Open Tableau Desktop**
2. **Connect to Amazon S3**
   - Server: `s3.amazonaws.com`
   - Bucket: `medicare-partd-analytics`
   - Folder: `processed/tableau_ready/main_dashboard/`

3. **Load CSV Extract**
   - Select the main dashboard CSV file
   - Tableau will automatically detect schema

### Recommended Visualizations

1. **Geographic Heat Map**: State-level opioid prescription rates
2. **Provider Scatter Plot**: Total claims vs. opioid percentage
3. **Time Series**: Temporal trends by risk category
4. **Top N Analysis**: Highest-prescribing providers by specialty
5. **ZIP Code Detail**: Drill-down to local hotspots

---

## Testing

### Extract Module Test

```bash
cd extract
python extract_s3_data.py
```

### Transform Module Test

```bash
cd transform
python transform_partd.py
```

### Load Module Test

```bash
cd load
python load_to_s3.py
```

---

## Data Sources

- **Medicare Part D Prescriber Data**: CMS.gov public use files
- **NPI Registry**: National Provider Identifier database
- **Census Population Data**: US Census Bureau
- **Drug Reference**: FDA/DEA classifications

---

**Gordan Tao**
- GitHub: [@gordantao](https://github.com/gordantao)
