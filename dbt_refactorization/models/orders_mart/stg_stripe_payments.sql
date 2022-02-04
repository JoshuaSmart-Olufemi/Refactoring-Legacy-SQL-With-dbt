
with payments as (

    select 
    order_id as order_id, 
    max(created) as payment_finalized_date, 
    sum(amount) / 100.0 as total_amount_paid
    from {{ source('dbt_josh_smart','stripe_payments') }}
    where STATUS <> 'fail'
    group by 1

)

select * from payments
