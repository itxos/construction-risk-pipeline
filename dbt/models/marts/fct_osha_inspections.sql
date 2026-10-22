with violations as (
    select
        activity_nr,
        count(*)                                            as citations,
        count(*) filter (where violation_type = 'serious')  as serious_citations,
        count(*) filter (where violation_type = 'repeat')   as repeat_citations,
        count(*) filter (where violation_type = 'willful')  as willful_citations,
        sum(current_penalty)                                as total_penalty_usd
    from {{ ref('stg_osha_violations') }}
    group by 1
)

select
    i.activity_nr,
    i.establishment_name,
    i.site_address,
    i.site_city,
    i.site_state,
    i.site_zip,
    i.naics_code,
    case left(i.naics_code, 3)
        when '236' then 'Construction of Buildings'
        when '237' then 'Heavy and Civil Engineering'
        when '238' then 'Specialty Trade Contractors'
        else 'Other'
    end                                                     as naics_subsector,
    i.employees_at_site,
    i.open_date,
    extract(year from i.open_date)::int                     as open_year,
    i.open_date < (current_date - interval '6 months')      as citations_mature,
    i.close_case_date,
    coalesce(v.citations, 0)                                as citations,
    coalesce(v.serious_citations, 0)                        as serious_citations,
    coalesce(v.repeat_citations, 0)                         as repeat_citations,
    coalesce(v.willful_citations, 0)                        as willful_citations,
    coalesce(v.total_penalty_usd, 0)                        as total_penalty_usd,
    coalesce(v.citations, 0) > 0                            as has_citations
from {{ ref('stg_osha_inspections') }} i
left join violations v on v.activity_nr = i.activity_nr
