-- mart_co2_forecast.sql
-- Calculates current CO2 baseline and projects future emissions.
-- Answers: "What should be our CO2 target if we want to increase
--           the amount of projects by 20% next year?"
--
-- Assumption: CO2 grows linearly with project count.
-- The average CO2 per project is used as the unit rate for new projects.
-- This is a conservative estimate — greener regions could lower the target.

with allocated as (
    select * from {{ ref('int_co2_allocated') }}
),

current_baseline as (
    select
        -- current state
        count(distinct project_id)                          as current_project_count,
        round(sum(co2_allocated_kgco2), 4)                  as current_total_co2_kgco2,

        -- average co2 per project (used as growth rate unit)
        round(
            sum(co2_allocated_kgco2) / count(distinct project_id),
            4
        )                                                   as avg_co2_per_project_kgco2

    from allocated
    where is_admin_project = false
),

forecast as (
    select
        current_project_count,
        current_total_co2_kgco2,
        avg_co2_per_project_kgco2,

        -- 20% growth scenario
        round(current_project_count * 1.20)                 as projected_project_count,
        round(current_project_count * 0.20)                 as new_projects_added,

        -- co2 target with linear growth assumption
        round(
            current_total_co2_kgco2
            + (current_project_count * 0.20 * avg_co2_per_project_kgco2),
            4
        )                                                   as projected_co2_kgco2,

        -- co2 increase in absolute terms
        round(
            current_project_count * 0.20 * avg_co2_per_project_kgco2,
            4
        )                                                   as co2_increase_kgco2,

        -- co2 increase as percentage
        round(20.0, 2)                                      as project_growth_pct,

        -- optimistic target: if new projects go to greenest regions
        -- assumes 30% efficiency gain by routing to greener infrastructure
        round(
            current_total_co2_kgco2
            + (current_project_count * 0.20 * avg_co2_per_project_kgco2 * 0.70),
            4
        )                                                   as optimistic_co2_target_kgco2

    from current_baseline
)

select * from forecast