{{
config(materialized = 'incremental',
incremental_strategy = 'delete+insert',
unique_key = ['control_id','valuation_date','source_table','source_measure_name'],
pre_hook= "Insert into stg_sch.log_table(model, status) values ('controls','started')",
post_hook= "Insert into stg_sch.log_table(model, status) values ('controls','ended')"
)
}}

select 1 as CONTROL_ID, src.VALUATION_DATE as VALUATION_DATE, 'TB_VS_FPSL' CONTROL_TYPE,
'TRAIL_BALANCE_TABLE' as SOURCE_TABLE, 'FPSL_TABLE' as TARGET_TABLE,SOURCE_MEASURE_NAME,TARGET_MEASURE_NAME,SOURCE_VALUE,TARGET_VALUE,SOURCE_VALUE-TARGET_VALUE DIFFERENCE_VALUE,
case when DIFFERENCE_VALUE=0 then 'Pass' else 'FAIL' end PASS_FAIL_FLAG, 0 as REMARKS, CURRENT_TIMESTAMP() CREATE_TS,CURRENT_TIMESTAMP() as UPDATE_TS, CURRENT_USER() as CREATE_USER,CURRENT_USER() as UPDATE_USER
from 
(select department SOURCE_MEASURE_NAME,VALUATION_DATE, sum(salary) as SOURCE_VALUE from trail_balance_table group by department,VALUATION_DATE) src
inner join (select department TARGET_MEASURE_NAME, VALUATION_DATE,sum(salary) as TARGET_VALUE from trail_balance_table group by department,VALUATION_DATE) tar 
on src.SOURCE_MEASURE_NAME  = tar.TARGET_MEASURE_NAME
and src.VALUATION_DATE= tar.VALUATION_DATE