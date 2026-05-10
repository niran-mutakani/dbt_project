{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
        schema = 'trans_sch'
    )
}}
select * from {{source('tb_fps_srcs','trail_balance_table')}}
where valuation_date = '{{var("val_date")}}'