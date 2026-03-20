-- stg_project_usage.sql
-- Staging model for the project-level SKU usage data.
-- Each row represents a project consuming a fraction of a SKU's total CO2.
-- Region metadata (geographic_zone, provider, timezone) is enriched here
-- via a join against the dim_regions seed table.
-- In production, dim_regions would be populated dynamically from the
-- Upsun regions API rather than a static seed file.

with source as (
    select * from {{ source('raw', 'raw_project_usage') }}
),

dim_regions as (
    select * from {{ ref('dim_regions') }}
),

renamed as (
    select
        -- identifiers
        src.provider,
        src.`SKU Description`                               as sku_description,
        src.`Product`                                       as project_id,

        -- region: remove '.platform.sh' suffix
        -- raw value example: 'eu-5.platform.sh' → cleaned: 'eu-5'
        replace(src.`Region`, '.platform.sh', '')           as region,

        -- region metadata from dim_regions seed
        -- source: https://developer.upsun.com/docs/development/regions
        coalesce(r.geographic_zone, 'Unknown')              as geographic_zone,
        coalesce(r.timezone, 'Unknown')                     as timezone,

        -- metrics: divide by 100 to get decimal proportion
        -- e.g. 0.587013 → 0.00587013
        src.`Emissions kgCO2 % of total emissions` / 100.0  as usage_pct_of_total,

        -- derived: flag admin/internal projects
        case
            when src.`Region` in (
                'admin.platform.sh',
                'us.dev.vpn.internal.platform.sh'
            ) then true
            else false
        end                                                 as is_admin_project

    from source src
    left join dim_regions r
        on replace(src.`Region`, '.platform.sh', '') = r.region_id

    where src.`Product` is not null
      and src.`Emissions kgCO2 % of total emissions` is not null
)

select * from renamed