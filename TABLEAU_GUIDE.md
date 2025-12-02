# Tableau Dashboard Guide - Medicare Part D Prescription Analytics

## Dashboard Overview

This guide walks through creating interactive Tableau dashboards to visualize Medicare Part D prescription data, focusing on high-risk prescriptions (especially opioids) and geographic hotspots.

---

## Getting Started

### Step 1: Connect to Data

1. **Open Tableau Desktop**
2. Click **Connect** → **To a Server** → **Amazon S3**
3. Enter credentials:
   - **Bucket**: `medicare-partd-analytics`
   - **Folder**: `processed/tableau_ready/main_dashboard/`
4. Select the CSV file and click **Open**

### Step 2: Data Source Setup

1. Tableau will auto-detect field types
2. Verify the following field types:
   - **npi**: String
   - **provider_state**: Geographic (State/Province)
   - **provider_zip**: String (not numeric)
   - **total_claims**: Number (whole)
   - **total_cost**: Number (decimal)
   - **opioid_percentage**: Number (decimal)

3. Create calculated fields (Analysis → Create Calculated Field):

```tableau
// Cost per Claim
[provider_total_cost] / [provider_total_claims]

// Opioid Claim Ratio
[provider_opioid_claims] / [provider_total_claims]

// High-Risk Flag
IF [opioid_percentage] > 25 THEN "High Risk"
ELSEIF [opioid_percentage] > 10 THEN "Moderate Risk"
ELSE "Low Risk"
END

// Claims per Beneficiary
[provider_total_claims] / [total_beneficiaries]
```

---

## Dashboard 1: Geographic Opioid Hotspots

### Visualization 1: State Heat Map

1. **Drag** `provider_state` to **Detail** on Marks card
2. **Right-click** → **Geographic Role** → **State/Province**
3. **Drag** `provider_state` to the view (should create a map)
4. **Drag** `SUM(provider_opioid_claims)` to **Color**
5. **Edit Colors**:
   - Palette: **Red-Orange**
   - Start: White
   - End: Dark Red
6. **Add Tooltip**:
   ```
   State: <provider_state>
   Total Opioid Claims: <SUM(provider_opioid_claims)>
   Opioid Providers: <COUNTD(npi)>
   Avg Opioid %: <AVG(opioid_percentage)>%
   ```

### Visualization 2: Top 10 States by Opioid Volume

1. **Create new sheet**
2. **Drag** `provider_state` to **Rows**
3. **Drag** `SUM(provider_opioid_claims)` to **Columns**
4. **Sort descending** (click sort icon)
5. **Filter**: Top 10 by Sum of Opioid Claims
6. **Add color**: Gradient based on opioid percentage
7. **Format**: Horizontal bar chart

### Visualization 3: ZIP Code Detail Map (Drill-down)

1. **Connect to**: `processed/hotspots/zip_opioid/` dataset
2. Create custom geocoding for ZIP codes
3. **Drag** `provider_zip` to **Detail**
4. **Drag** `provider_state` to **Detail**
5. **Drag** `SUM(zip_opioid_claims)` to **Size**
6. **Drag** `SUM(zip_opioid_claims)` to **Color**
7. Use **circle marks** for ZIP code points

### Combine into Dashboard

1. **Dashboard** → **New Dashboard**
2. **Size**: Automatic (responsive)
3. **Layout**:
   - Top: State heat map (full width)
   - Bottom Left: Top 10 states bar chart
   - Bottom Right: ZIP detail map
4. **Add Filters**:
   - State filter (apply to all sheets)
   - Opioid percentage range slider
5. **Add Actions**:
   - **Filter Action**: Click state on map → filters other views
   - **Highlight Action**: Hover to highlight related data

---

## Dashboard 2: Provider Analysis

### Visualization 1: Provider Scatter Plot

1. **Columns**: `SUM(provider_total_claims)`
2. **Rows**: `AVG(opioid_percentage)`
3. **Detail**: `npi`, `provider_name`
4. **Size**: `SUM(total_beneficiaries)`
5. **Color**: High-Risk Flag (calculated field)
6. **Add Trend Line**: Analysis → Trend Lines → Show Trend Lines
7. **Add Reference Line**: 
   - Y-axis at 25% (high-risk threshold)
   - Label: "High-Risk Threshold"

### Visualization 2: Top Prescribers Table

1. **Rows**: `provider_name`, `provider_city`, `provider_state`, `specialty`
2. **Text**: 
   - `SUM(provider_total_claims)`
   - `SUM(provider_opioid_claims)`
   - `AVG(opioid_percentage)`
   - `COUNTD(unique_drugs_prescribed)`
3. **Sort**: By total claims (descending)
4. **Filter**: Top 100 providers
5. **Format**: 
   - Banded rows
   - Conditional formatting (red for opioid % > 25%)

### Visualization 3: Specialty Distribution

1. **Columns**: `specialty`
2. **Rows**: `SUM(provider_total_claims)`
3. **Color**: `risk_category` (if using specialty_risk dataset)
4. **Sort**: Descending by claims
5. **Chart Type**: Stacked bar chart

---

## Dashboard 3: Temporal Trends

### Visualization 1: Year-over-Year Line Chart

1. **Connect to**: `processed/temporal/yearly_trends/` dataset
2. **Columns**: `report_year`
3. **Rows**: `SUM(yearly_claims)`
4. **Color**: `risk_category`
5. **Add Reference Line**: Show average
6. **Format**: Multi-line chart

### Visualization 2: Quarterly Comparison

1. **Columns**: `report_quarter`
2. **Rows**: `SUM(yearly_cost)`
3. **Color**: `provider_state`
4. **Filter**: Select specific states to compare
5. **Chart Type**: Line chart with markers

---

## Dashboard 4: Drug-Level Analysis

### Visualization 1: Top Drugs by Volume

1. **Connect to**: `processed/tableau_ready/drug_summary/` dataset
2. **Rows**: `drug_name`
3. **Columns**: `SUM(drug_total_claims)`
4. **Color**: `risk_category`
5. **Filter**: Top 25 drugs
6. **Add Tooltip**: Show prescriber count, avg cost

### Visualization 2: Drug Cost Analysis

1. **Columns**: `SUM(drug_total_claims)`
2. **Rows**: `AVG(avg_cost_per_claim)`
3. **Detail**: `drug_name`, `generic_name`
4. **Size**: `SUM(total_patients)`
5. **Color**: `risk_category`
6. **Add quadrant reference lines**:
   - High volume, high cost
   - High volume, low cost
   - Low volume, high cost
   - Low volume, low cost

---

## Design Best Practices

### Color Scheme

- **Opioids**: Red spectrum (#8B0000 to #FF6347)
- **Benzodiazepines**: Orange (#FF8C00 to #FFA500)
- **Stimulants**: Yellow (#FFD700)
- **Standard**: Blue-grey (#4682B4)

### Formatting

1. **Number Format**:
   - Claims: `#,##0` (thousands separator)
   - Cost: `$#,##0.00`
   - Percentages: `0.0%`

2. **Fonts**:
   - Headers: Tableau Bold, 14pt
   - Body: Tableau Regular, 10pt
   - Labels: 9pt

3. **Tooltips**:
   - Keep concise (4-5 key metrics)
   - Use `<>` for dynamic fields
   - Format numbers consistently

---

## Filters and Parameters

### Global Filters (Apply to All Dashboards)

1. **State Filter**: Multi-select dropdown
2. **Year Filter**: Slider or dropdown
3. **Risk Category**: Checkbox (Opioid, Benzo, Stimulant, Standard)

### Dynamic Parameters

Create parameter for "Hotspot Threshold":
- **Data Type**: Float
- **Range**: 0.75 to 0.99
- **Step**: 0.01
- **Current Value**: 0.95

Use in calculated field:
```tableau
// Is Hotspot
IF [State Opioid Rank] >= [Hotspot Threshold Parameter]
THEN "Hotspot"
ELSE "Normal"
END
```

---

## Publishing and Sharing

### Publish to Tableau Server/Online

1. **Server** → **Publish Workbook**
2. **Set Permissions**: 
   - Viewers: Can view only
   - Analysts: Can view and download
3. **Schedule Refresh**: Daily at 2 AM
4. **Embed in Web**:
   ```html
   <iframe src="https://tableau-server/views/PartD/Dashboard1" 
           width="100%" height="800"></iframe>
   ```

### Export Options

- **PDF**: Dashboard → Export → PDF
- **PowerPoint**: Dashboard → Export → PowerPoint
- **Image**: Dashboard → Export → Image

---

## Advanced Features

### Custom SQL for Additional Analysis

```sql
SELECT 
    provider_state,
    COUNT(DISTINCT npi) as provider_count,
    SUM(provider_opioid_claims) as total_opioid_claims,
    AVG(opioid_percentage) as avg_opioid_pct
FROM main_dashboard
WHERE provider_total_claims > 100
GROUP BY provider_state
HAVING avg_opioid_pct > 15
ORDER BY total_opioid_claims DESC
```

### LOD Expressions

```tableau
// State Average Opioid Percentage
{ FIXED [provider_state] : AVG([opioid_percentage]) }

// Provider Rank Within State
{ FIXED [provider_state] : RANK([provider_opioid_claims]) }
```

---

**Last Updated**: December 2025  
**Tableau Version**: 2023.3+  
**Author**: Gordon Tao
