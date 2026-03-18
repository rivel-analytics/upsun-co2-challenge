-- mart_co2_by_region.sql
-- Aggregates CO2 emissions and intensity per region.
-- Answers: "What regions are greener?"
-- A greener region has lower CO2 per project (less emissions for the same usage).

with allocated as (
    select * from {{ ref('int_co2_allocated') }}
),

region_stats as (
    select
        region,
        provider,

        -- total co2 for this region
        round(sum(co2_allocated_kgco2), 2)              as total_co2_kg,

        -- number of distinct projects in this region
        count(distinct project_id)                       as project_count,

        -- number of distinct SKUs consumed in this region
        count(distinct sku_description)                  as sku_count,

        -- co2 intensity: kg per project
        -- this is the key "greenness" metric:
        -- lower = greener (less CO2 per project for the same workload)
        round(
            sum(co2_allocated_kgco2) / count(distinct project_id),
            4
        )                                                as co2_per_project_kg,

        -- breakdown by usage category
        round(sum(case when usage_category = 'Compute'
            then co2_allocated_kgco2 else 0 end), 2)    as co2_compute_kg,
        round(sum(case when usage_category = 'Storage'
            then co2_allocated_kgco2 else 0 end), 2)    as co2_storage_kg,
        round(sum(case when usage_category = 'Data Transfer'
            then co2_allocated_kgco2 else 0 end), 2)    as co2_data_transfer_kg

    from allocated
    where 
        is_admin_project = false  -- exclude internal admin projects
        and region != 'us.dev.vpn.internal' -- exclude internal admin projects

    group by
        region,
        provider
),

final as (
    select
        *,
        -- percentage of total company CO2
        round(
            total_co2_kg / sum(total_co2_kg) over () * 100,
            4
        )                                                as pct_of_total_co2,

        -- rank regions by co2 intensity (1 = greenest)
        rank() over (order by co2_per_project_kg asc) as greenness_rank,
        case
            when rank() over (order by co2_per_project_kg asc) = 1 then 'Lowest Carbon'
            when rank() over (order by co2_per_project_kg asc) = 2 then 'Very Low Carbon'
            when rank() over (order by co2_per_project_kg asc) <= 4 then 'Low Carbon'
            when rank() over (order by co2_per_project_kg asc) <= 6 then 'Medium Carbon'
            when rank() over (order by co2_per_project_kg asc) <= 8 then 'High Carbon'
            else 'Very High Carbon'
        end                                             as greenness

    from region_stats
)

select * from final
order by co2_per_project_kg asc