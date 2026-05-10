select * from {{ref('finance_controls_results')}} where PASS_FAIL_FLAG = 'FAIL'
and valuation_date = '{{var("val_date")}}' and control_type = '{{var("control_type")}}'