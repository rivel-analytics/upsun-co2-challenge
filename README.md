# CO2 Accounting Pipeline — Upsun AE Challenge

A production-grade Analytics Engineering pipeline that cascades cloud provider 
CO2 emissions down to individual project level, enabling ESG reporting and 
data-driven sustainability decisions.

## Stack

- **Data Warehouse:** Google BigQuery
- **Transformation:** dbt Core 1.11 + dbt-bigquery
- **Reporting:** Metabase (connected to BigQuery marts)
- **Source data:** AWS billing SKUs + project usage percentages

## Architecture
```
Raw CSVs
    ↓ (manual load)
BigQuery raw.*
    ↓
dbt staging     → cleaned & typed views
    ↓
dbt intermediate → CO2 cascade allocation logic
    ↓
dbt marts       → aggregated tables for ESG stakeholders
    ↓
Metabase        → dashboards & reports
```

## Data Model

### Cascade allocation logic

Each row in `raw_project_usage` represents a project consuming a percentage 
of a SKU's total CO2. The allocation formula is:
```
co2_allocated_kgco2 = sku.total_emissions_kgco2 × project.usage_pct_of_total
```

### Models

| Layer | Model | Type | Description |
|-------|-------|------|-------------|
| Staging | `stg_sku_bills` | View | Cleaned SKU billing data with aggregated CO2 per SKU |
| Staging | `stg_project_usage` | View | Project-level SKU usage as decimal proportions |
| Intermediate | `int_co2_allocated` | View | CO2 cascade: joins SKUs with projects |
| Mart | `mart_co2_by_project` | Table | Total CO2 per project with category breakdown |
| Mart | `mart_co2_by_region` | Table | CO2 intensity per region (greenness ranking) |
| Mart | `mart_co2_forecast` | Table | +20% project growth scenario forecast |

## Business Questions Answered

**1. What should we do to reduce CO2 emissions?**
→ Compute drives 71.6% of all emissions. Migrating high-emitting projects 
from `eu.platform.sh` (0.63 kg/project) to `eu-5.platform.sh` (0.01 kg/project) 
would yield up to 54x reduction in CO2 intensity.

**2. What regions are greener?**
→ `eu-5.platform.sh` is the greenest region at 0.0116 kgCO2/project.
`eu.platform.sh` is the least efficient at 0.6287 kgCO2/project — 54x higher.

**3. What should be our CO2 target with +20% projects?**
→ Current baseline: 430.97 kgCO2 across 4,322 projects.
Linear growth target: **517.15 kgCO2** (+86.18 kg).
Optimistic target (new projects to greener regions): **491.29 kgCO2** (+60.32 kg).

## Data Quality

36 tests implemented across all layers. 3 known warnings from source data:

| Warning | Root cause | Action required |
|---------|-----------|-----------------|
| `unique_stg_sku_bills_sku_description` | Same SKU exists with multiple `product_category` variants in source | Provider should include `product_category` in billing exports |
| `relationships` (4411 rows) | SKUs in project_usage not found in sku_bills after aggregation | Same root cause as above |
| `assert_total_co2_allocation` | Slight deviation from 100% due to unmatched SKUs | Same root cause as above |

**Modeling decision:** `stg_sku_bills` aggregates all `product_category` variants 
of the same SKU into a single row (summing emissions). This is the most defensible 
approach given that `raw_project_usage` does not include `product_category`.

## Setup

### Prerequisites
- Python 3.8+
- dbt-bigquery: `pip install dbt-bigquery`
- Google Cloud SDK with BigQuery access
- BigQuery project with `raw` and `co2_analytics` datasets

### Authentication
```bash
gcloud auth application-default login
```

### Run the pipeline
```bash
dbt run        # build all models
dbt test       # run all 36 tests
dbt docs serve # generate and serve documentation
```

### Run specific layers
```bash
dbt run --select staging
dbt run --select intermediate
dbt run --select marts
```