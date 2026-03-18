-- mart_co2_by_project.sql
-- Aggregates total CO2 emissions per project.
-- Answers: "What should we do to reduce our CO2 emissions?"
-- by identifying the highest-emitting projects and their usage patterns.

with allocated as (
    select * from {{ ref('int_co2_allocated') }}
),

aggregated as (
    select
        project_id,
        region,
        is_admin_project,
        provider,

        -- total co2 for this project across all SKUs
        round(sum(co2_allocated_kgco2), 4)          as total_co2_kg,

        -- breakdown by usage category
        round(sum(case when usage_category = 'Compute'
            then co2_allocated_kgco2 else 0 end), 4) as co2_compute_kg,
        round(sum(case when usage_category = 'Storage'
            then co2_allocated_kgco2 else 0 end), 4) as co2_storage_kg,
        round(sum(case when usage_category = 'Data Transfer'
            then co2_allocated_kgco2 else 0 end), 4) as co2_data_transfer_kg,
        round(sum(case when usage_category = 'Other'
            then co2_allocated_kgco2 else 0 end), 4) as co2_other_kg,

        -- number of distinct SKUs consumed by this project
        count(distinct sku_description)              as sku_count,

        -- flag if project has any uncategorized SKUs
        logical_or(is_uncategorized)                 as has_uncategorized_skus

    from allocated
    group by
        project_id,
        region,
        is_admin_project,
        provider
),

final as (
    select
        *,
        -- percentage of total company CO2
        round(
            total_co2_kg / sum(total_co2_kg) over () * 100,
            4
        )                                            as pct_of_total_co2,

        -- rank projects by emissions (1 = highest emitter)
        rank() over (
            order by total_co2_kg desc
        )                                            as co2_rank

    from aggregated
)

select * from final
order by total_co2_kg desc