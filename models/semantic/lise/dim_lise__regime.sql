with regimes as (
    select * from {{ ref('stg_lise__regimes') }}
)

select
    RegimeID as RegimeKey,
    RegimeID,
    Regime
from regimes

union all

select
    -1 as RegimeKey,
    cast(null as bigint) as RegimeID,
    'Unknown' as Regime