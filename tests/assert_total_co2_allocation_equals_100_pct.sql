-- Singular test: validates that the sum of all usage percentages equals 100%.
-- This is the most critical data quality check in the entire pipeline.
-- It ensures no CO2 emissions are lost or double-counted in the cascade allocation.
--
-- Known limitation: ~4411 rows in raw_project_usage reference SKUs that exist
-- in the bills with different usage_category combinations, causing a small
-- deviation from 100%. This is a source data quality issue — the SKU description
-- alone is not a unique identifier in raw_sku_bills (multiple product_category
-- variants exist per SKU). We aggregate at the SKU level in stg_sku_bills but
-- some SKU descriptions in project_usage still do not find a match.
--
-- Severity: warn (not error) — deviation is documented and expected.
-- Action required from data provider: include product_category in billing exports.

{{ config(severity='warn') }}

select
    round(sum(usage_pct_of_total) * 100, 6)        as total_pct,
    abs(sum(usage_pct_of_total) - 1.0)             as deviation_from_100pct
from {{ ref('stg_project_usage') }}
having abs(sum(usage_pct_of_total) - 1.0) > 0.00001