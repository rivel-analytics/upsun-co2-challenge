-- stg_sku_bills.sql
-- Staging model for the provider SKU bills with their CO2 emissions.
--
-- Data quality consideration: the same SKU description can appear multiple times
-- with different product_category values (grid, dedicated, Uncategorized),
-- each with different emission totals. Since raw_project_usage only references
-- sku_description (without product_category), we aggregate all variants of the
-- same SKU into a single row by summing their total emissions.
--
-- This is a conscious modeling decision: the project-level usage percentage
-- applies to the combined total emissions of all infrastructure variants of that SKU.

with source as (
    select * from {{ source('raw', 'raw_sku_bills') }}
),

renamed as (
    select
        provider,
        `SKU Description`                   as sku_description,
        `Usage category`                    as usage_category,

        -- aggregate emissions across all product_category variants
        sum(`Total emissions kgCO2`)        as total_emissions_kgco2,

        -- flag if any variant is uncategorized
        logical_or(
            `Product category` = 'Uncategorized'
        )                                   as is_uncategorized,

        -- count how many variants exist for visibility
        count(*)                            as product_category_variants

    from source
    where `Total emissions kgCO2` is not null
    group by
        provider,
        `SKU Description`,
        `Usage category`
)

select * from renamed