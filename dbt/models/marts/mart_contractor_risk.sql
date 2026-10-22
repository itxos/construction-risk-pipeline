with scored as (
    select
        contractor_key,
        contractor_name,
        mature_inspections                     as inspections,
        cities,
        first_inspection,
        last_inspection,
        mature_serious_citations               as serious_citations,
        mature_repeat_citations                as repeat_citations,
        mature_willful_citations               as willful_citations,
        total_penalty_usd,
        -- Weights are an assumption (see README): serious = 1, repeat = 3, willful = 5, other = 0
        mature_serious_citations * 1
          + mature_repeat_citations * 3
          + mature_willful_citations * 5       as risk_points
    from {{ ref('dim_contractor') }}
    -- Businesses only, and only inspections old enough to have had their citations posted.
    where is_business
      and mature_inspections >= 3
)

select
    *,
    round(risk_points::numeric / inspections, 2)  as risk_points_per_inspection,
    rank() over (order by risk_points desc)       as risk_rank
from scored
