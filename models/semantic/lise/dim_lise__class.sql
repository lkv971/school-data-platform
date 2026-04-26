with classes as (
    select * from {{ ref('stg_lise__classes') }}
),
niveaux as (
    select * from {{ ref('stg_lise__niveaux') }}
),
etablissements as (
    select * from {{ ref('stg_lise__etablissements') }}
),

actual_classes as (
    select distinct
        cast(concat('CLASS|', cast(c.ClasseID as varchar(50))) as varchar(100)) as ClassKey,
        cast(c.ClasseID as bigint) as ClasseID,
        cast(coalesce(c.Classe, 'Unknown') as varchar(255)) as Classe,
        cast(coalesce(c.ClasseLibelle, 'Unknown') as varchar(255)) as ClasseLibelle,
        cast(c.NiveauID as bigint) as NiveauID,
        cast(coalesce(n.Niveau, 'Unknown') as varchar(255)) as Niveau,
        cast(c.EtablissementID as bigint) as EtablissementID,
        cast(coalesce(e.Etablissement, 'Unknown') as varchar(255)) as Etablissement,
        cast(0 as int) as IsUnassigned
    from classes c
    left join niveaux n
        on c.NiveauID = n.NiveauID
    left join etablissements e
        on c.EtablissementID = e.EtablissementID
    where c.ClasseID is not null
),

unassigned_niveau_rows as (
    select distinct
        cast(
            concat(
                'NOCLASS|',
                cast(n.NiveauID as varchar(50)),
                '|',
                coalesce(cast(n.EtablissementID as varchar(50)), 'NA')
            ) as varchar(100)
        ) as ClassKey,
        cast(null as bigint) as ClasseID,
        cast('Unassigned' as varchar(255)) as Classe,
        cast('Not linked to a class' as varchar(255)) as ClasseLibelle,
        cast(n.NiveauID as bigint) as NiveauID,
        cast(coalesce(n.Niveau, 'Unknown') as varchar(255)) as Niveau,
        cast(n.EtablissementID as bigint) as EtablissementID,
        cast(coalesce(e.Etablissement, 'Unknown') as varchar(255)) as Etablissement,
        cast(1 as int) as IsUnassigned
    from niveaux n
    left join etablissements e
        on n.EtablissementID = e.EtablissementID
    where n.NiveauID is not null
),

unassigned_etablissement_rows as (
    select distinct
        cast(concat('NOCLASS|NOLEVEL|', cast(e.EtablissementID as varchar(50))) as varchar(100)) as ClassKey,
        cast(null as bigint) as ClasseID,
        cast('Unassigned' as varchar(255)) as Classe,
        cast('Not linked to a class or level' as varchar(255)) as ClasseLibelle,
        cast(null as bigint) as NiveauID,
        cast('Unknown' as varchar(255)) as Niveau,
        cast(e.EtablissementID as bigint) as EtablissementID,
        cast(coalesce(e.Etablissement, 'Unknown') as varchar(255)) as Etablissement,
        cast(1 as int) as IsUnassigned
    from etablissements e
    where e.EtablissementID is not null
),

unknown_row as (
    select
        cast('UNKNOWN' as varchar(100)) as ClassKey,
        cast(null as bigint) as ClasseID,
        cast('Unknown' as varchar(255)) as Classe,
        cast('Unknown' as varchar(255)) as ClasseLibelle,
        cast(null as bigint) as NiveauID,
        cast('Unknown' as varchar(255)) as Niveau,
        cast(null as bigint) as EtablissementID,
        cast('Unknown' as varchar(255)) as Etablissement,
        cast(1 as int) as IsUnassigned
)

select * from actual_classes
union all
select * from unassigned_niveau_rows
union all
select * from unassigned_etablissement_rows
union all
select * from unknown_row