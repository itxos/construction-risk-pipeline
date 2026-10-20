select r.contractor_key
from {{ ref('mart_contractor_risk') }} r
join {{ ref('dim_contractor') }} d using (contractor_key)
where not d.is_business
