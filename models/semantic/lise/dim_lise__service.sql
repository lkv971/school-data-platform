with services as (
    select * from {{ ref('stg_lise__services') }}
)

select
    ServiceID as ServiceKey,
    ServiceID,
    Service
from services

union all

select
    -1 as ServiceKey,
    cast(null as bigint) as ServiceID,
    'Unknown' as Service