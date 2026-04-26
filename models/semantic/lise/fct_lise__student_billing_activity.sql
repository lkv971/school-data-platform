with student_billing as (
    select * from {{ ref('stg_lise__factures_eleves') }}
)

select
    (year(try_cast(DateFacture as date)) * 100 + month(try_cast(DateFacture as date))) as MonthKey,

    case
        when ClasseID is not null then concat('CLASS|', cast(ClasseID as varchar(50)))
        else 'UNKNOWN'
    end as ClassKey,

    coalesce(
        nullif(ltrim(rtrim(cast(EleveKey as varchar(100)))), ''),
        'UNKNOWN_STUDENT'
    ) as StudentKey,

    coalesce(try_cast(RegimeID as bigint), -1) as RegimeKey,

    coalesce(try_cast(TotalEleve as decimal(18,2)), 0) as BilledAmount
from student_billing
where try_cast(DateFacture as date) is not null