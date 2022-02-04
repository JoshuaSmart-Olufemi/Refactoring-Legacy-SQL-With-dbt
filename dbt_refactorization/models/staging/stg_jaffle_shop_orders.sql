with orders as (

    select * 
    from {{ source('dbt_josh_smart','jaffle_shop_orders') }}
)

select * from orders 