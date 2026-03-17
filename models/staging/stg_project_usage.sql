-- stg_project_usage.sql
-- Staging model for the project-level SKU usage data.
-- Each row represents a project consuming a fraction of a SKU's total CO2.

with source as (
    select * from {{ source('raw', 'raw_project_usage') }}
),

renamed as (
    select
        -- identifiers
        provider,
        `SKU Description`                               as sku_description,
        `Product`                                       as project_id,
        `Region`                                        as region,

        -- metrics: BigQuery already imported this as FLOAT64 (e.g. 0.587013)
        -- we just divide by 100 to get the decimal proportion
        `Emissions kgCO2 % of total emissions` / 100.0  as usage_pct_of_total,

        -- derived: flag admin/internal projects
        case
            when `Region` = 'admin.platform.sh' then true
            else false
        end                                             as is_admin_project

    from source
    where `Product` is not null
      and `Emissions kgCO2 % of total emissions` is not null
)

select * from renamed