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
        round(sum(co2_allocated_kgco2), 4)              as total_co2_kgco2,

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
        )                                                as co2_per_project_kgco2,

        -- breakdown by usage category
        round(sum(case when usage_category = 'Compute'
            then co2_allocated_kgco2 else 0 end), 4)    as co2_compute_kgco2,
        round(sum(case when usage_category = 'Storage'
            then co2_allocated_kgco2 else 0 end), 4)    as co2_storage_kgco2,
        round(sum(case when usage_category = 'Data Transfer'
            then co2_allocated_kgco2 else 0 end), 4)    as co2_data_transfer_kgco2

    from allocated
    where is_admin_project = false  -- exclude internal admin projects
    group by
        region,
        provider
),

final as (
    select
        *,
        -- percentage of total company CO2
        round(
            total_co2_kgco2 / sum(total_co2_kgco2) over () * 100,
            4
        )                                                as pct_of_total_co2,

        -- rank regions by co2 intensity (1 = greenest)
        rank() over (
            order by co2_per_project_kgco2 asc
        )                                                as greenness_rank

    from region_stats
)

select * from final
order by co2_per_project_kgco2 asc