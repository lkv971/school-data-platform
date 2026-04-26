with payroll as (
    select * from {{ ref('mart_lise__payroll_monthly') }}
)

select
    (CalendarYear * 100 + CalendarMonth) as MonthKey,

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

    coalesce(ProfessionTypeID, -1) as ProfessionTypeKey,

    PayrollLineCount,
    PersonnelCount,
    TotalPayrollAmount,
    AvgPayrollAmount
from payroll