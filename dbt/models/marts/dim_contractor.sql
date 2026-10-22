with base as (
    select
        *,
        {{ normalize_company_name('establishment_name') }} as name_norm,
        -- Privacy-first heuristic: a name counts as a business only if it carries a business marker.
        -- Ambiguous names are treated as individuals and kept out of ranked output.
        upper(establishment_name) ~ '\m(LLC|INC|LP|LLP|LTD|CORP|CORPORATION|COMPANY|CO|CONSTRUCTION|CONTRACTORS?|BUILDERS?|BUILDING|DESIGN|DEVELOPMENT|DEVELOPERS?|ROOFING|ROOFERS?|ELECTRIC|ELECTRICAL|PLUMBING|HVAC|MECHANICAL|CONCRETE|MASONRY|INSULATION|PAINTING|FRAMING|DRYWALL|FENCE|FENCING|REMODELING|FLOORING|PAVING|EXCAVATION|DEMOLITION|WELDING|STEEL|GLASS|SIDING|STUCCO|FOUNDATION|LANDSCAP\w*|SOLAR|ENGINEERING|EQUIPMENT|INTERIORS|CARPENTRY|SERVICES?|ENTERPRISES?|GROUP|INDUSTRIES|PARTNERS|SOLUTIONS|HOMES|TRUCKING)\M'
            or establishment_name like '%&%'
            as looks_like_business
    from {{ ref('fct_osha_inspections') }}
)

select
    md5(name_norm)                                  as contractor_key,
    min(establishment_name)                         as contractor_name,
    name_norm,
    bool_or(looks_like_business)                    as is_business,
    count(*)                                        as inspections,
    count(distinct site_city)                       as cities,
    min(open_date)                                  as first_inspection,
    max(open_date)                                  as last_inspection,
    sum(citations)                                  as citations,
    sum(serious_citations)                          as serious_citations,
    sum(repeat_citations)                           as repeat_citations,
    sum(willful_citations)                          as willful_citations,
    count(*) filter (where citations_mature)                                          as mature_inspections,
    coalesce(sum(serious_citations) filter (where citations_mature), 0)               as mature_serious_citations,
    coalesce(sum(repeat_citations)  filter (where citations_mature), 0)               as mature_repeat_citations,
    coalesce(sum(willful_citations) filter (where citations_mature), 0)               as mature_willful_citations,
    sum(total_penalty_usd)                          as total_penalty_usd
from base
group by name_norm
