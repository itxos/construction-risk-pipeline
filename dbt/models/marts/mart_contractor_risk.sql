with scored as (
    select
        contractor_key,
        contractor_name,
        inspections,
        cities,
        first_inspection,
        last_inspection,
        citations,
        serious_citations,
        repeat_citations,
        willful_citations,
        total_penalty_usd,
        -- Weights are an assumption (see README): serious = 1, repeat = 3, willful = 5, other = 0
        serious_citations * 1 + repeat_citations * 3 + willful_citations * 5 as risk_points
    from {{ ref('dim_contractor') }}
    -- Ranked output covers businesses only, with enough inspections to avoid one-off headlines.
    where is_business
      and inspections >= 3
)

select
    *,
    round(risk_points::numeric / inspections, 2)  as risk_points_per_inspection,
    rank() over (order by risk_points desc)       as risk_rank
from scored
