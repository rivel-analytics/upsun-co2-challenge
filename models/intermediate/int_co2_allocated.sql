-- int_co2_allocated.sql
-- Core transformation: allocates CO2 emissions from SKUs down to individual projects.
--
-- The logic (cascade):
--   co2_allocated_kgco2 = sku.total_emissions_kgco2 * project.usage_pct_of_total
--
-- Note on data quality: stg_sku_bills aggregates multiple product_category variants
-- (grid, dedicated, Uncategorized) of the same SKU into a single row by summing
-- their emissions. This is necessary because raw_project_usage only references
-- sku_description without product_category, making it impossible to join at
-- the variant level. The aggregated total is the correct denominator for the
-- percentage-based allocation.
--
-- Materialization note: this model is a view. In production it could be
-- ephemeral (pure CTE) since it is transformation logic not intended for
-- direct stakeholder consumption.

with sku_bills as (
    select * from {{ ref('stg_sku_bills') }}
),

project_usage as (
    select * from {{ ref('stg_project_usage') }}
),

co2_allocated as (
    select
        -- project identifiers
        p.project_id,
        p.region,
        p.geographic_zone,
        p.timezone,
        p.is_admin_project,

        -- sku details
        p.provider,
        p.sku_description,
        s.usage_category,
        s.is_uncategorized,
        s.product_category_variants,

        -- co2 cascade: core calculation
        -- usage_pct_of_total is a decimal proportion (e.g. 0.00587)
        -- multiplied by the aggregated SKU total gives project-level allocation
        round(
            s.total_emissions_kgco2 * p.usage_pct_of_total,
            6
        )                                       as co2_allocated_kgco2

    from project_usage      p
    inner join sku_bills     s
        on  p.provider        = s.provider
        and p.sku_description = s.sku_description
)

select * from co2_allocated