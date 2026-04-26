with payroll as (
    select * from {{ ref('stg_lise__payroll') }}
)

select
    (year([Date]) * 100 + month([Date])) as MonthKey,

    case
        when ClasseID is not null then concat('CLASS|', cast(ClasseID as varchar(50)))
        when NiveauID is not null then concat(
            'NOCLASS|',
            cast(NiveauID as varchar(50)),
            '|',
            coalesce(cast(EtablissementID as varchar(50)), 'NA')
        )
        when EtablissementID is not null then concat('NOCLASS|NOLEVEL|', cast(EtablissementID as varchar(50)))
        else 'UNKNOWN'
    end as ClassKey,

    coalesce(cast(PersonnelID as varchar(100)), 'UNKNOWN_PERSONNEL') as PersonnelKey,
    coalesce(cast(ProfessionTypeID as bigint), -1) as ProfessionTypeKey,
    cast(Montant as decimal(18,2)) as PayrollAmount
from payroll