select
    activity_nr::text || '-' || citation_id                as violation_key,
    activity_nr,
    citation_id,
    payload ->> 'standard'                                 as standard_cited,
    payload ->> 'viol_type'                                as violation_type_code,
    case payload ->> 'viol_type'
        when 'S'   then 'serious'
        when 'W'   then 'willful'
        when 'R'   then 'repeat'
        when 'O'   then 'other'
        when 'U'   then 'unclassified'
        when 'FTA' then 'failure_to_abate'
        else 'unknown'
    end                                                    as violation_type,
    (payload ->> 'issuance_date')::timestamp::date         as issuance_date,
    (payload ->> 'contest_date')::timestamp::date          as contest_date,
    (payload ->> 'initial_penalty')::numeric               as initial_penalty,
    (payload ->> 'current_penalty')::numeric               as current_penalty,
    nullif(payload ->> 'nr_instances', '')::int            as instances,
    nullif(payload ->> 'nr_exposed', '')::int              as employees_exposed,
    ingested_at
from {{ source('raw', 'osha_violations') }}
-- Deleted citations are excluded: they carry penalties that were never owed.
where coalesce(payload ->> 'delete_flag', '') <> 'X'
