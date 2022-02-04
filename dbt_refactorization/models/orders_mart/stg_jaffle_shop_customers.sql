with customers as (

    select * from {{ source('dbt_josh_smart','jaffle_shop_customers')}}

)

select * from customers