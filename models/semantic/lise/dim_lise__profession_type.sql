with profession_types as (
    select * from {{ ref('stg_lise__profession_types') }}
)

select
    ProfessionTypeID as ProfessionTypeKey,
    ProfessionTypeID,
    ProfessionType
from profession_types

union all

select
    -1 as ProfessionTypeKey,
    cast(null as bigint) as ProfessionTypeID,
    'Unknown' as ProfessionType