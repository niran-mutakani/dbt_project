{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
        schema = 'trans_sch'
    )
}}
select * from {{source('tb_fps_srcs','fpsl_table')}}
where valuation_date = '{{var("val_date")}}'