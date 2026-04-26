-- Auto Generated (Do not modify) 3B7BBCC74491B1C40156617241DDEB01651BAB8D11CBC97AA9A6F0131DFC9712
create view "dbt_school_dev"."stg_lise__services" as select
    ServiceID,
    Service
from "WH_GOLD"."LISE"."Services";