with monthly as (
    select
        date_trunc('month', open_date)::date                  as open_month,
        naics_subsector,
        count(*)                                              as inspections,
        count(*) filter (where has_citations)                 as inspections_with_citations,
        round(100.0 * count(*) filter (where has_citations)
              / count(*), 1)                                  as pct_with_citations,
        sum(citations)                                        as citations,
        sum(serious_citations)                                as serious_citations,
        sum(repeat_citations)                                 as repeat_citations,
        sum(willful_citations)                                as willful_citations,
        sum(total_penalty_usd)                                as total_penalty_usd
    from {{ ref('fct_osha_inspections') }}
    group by 1, 2
)

select
    *,
    -- Citations post months after an inspection opens, so recent months undercount them.
    open_month < date_trunc('month', current_date - interval '6 months')::date as is_mature_month
from monthly
