WITH paid_orders as (

    select 
    orders.id as order_id,
    orders.user_id	as customer_id,
    orders.order_date AS order_placed_at,
    orders.STATUS AS order_status,
    p.total_amount_paid,
    p.payment_finalized_date,
    C.FIRST_NAME as customer_first_name,
    C.LAST_NAME as customer_last_name
FROM {{ source('dbt_josh_smart','jaffle_shop_orders') }} as orders
left join (select 
           order_id as order_id, 
           max(created) as payment_finalized_date, 
           sum(amount) / 100.0 as total_amount_paid
    from {{ source('dbt_josh_smart','stripe_payments') }}
    where STATUS <> 'fail'
    group by 1) p ON orders.id = p.order_id
left join {{ source('dbt_josh_smart','jaffle_shop_customers')}} C on orders.user_id = C.id )
,

customer_orders as (

    select C.id as customer_id
    , min(order_date) as first_order_date
    , max(order_date) as most_recent_order_date
    , count(orders.id) AS number_of_orders
from {{ source('dbt_josh_smart','jaffle_shop_customers')}} C 
left join {{ source('dbt_josh_smart','jaffle_shop_orders') }}  as orders
on orders.user_id = C.id 
group by 1)

select
p.*,
ROW_NUMBER() OVER (ORDER BY p.order_id) as transaction_seq,
ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY p.order_id) as customer_sales_seq,
CASE WHEN c.first_order_date = p.order_placed_at
THEN 'new'
ELSE 'return' END as nvsr,
x.clv_bad as customer_lifetime_value,
c.first_order_date as fdos
FROM paid_orders p
left join customer_orders as c USING (customer_id)
LEFT OUTER JOIN 
(
    select
    p.order_id,
    sum(t2.total_amount_paid) as clv_bad
    from paid_orders p
    left join paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
    group by 1
    order by p.order_id
) x on x.order_id = p.order_id
ORDER BY order_id 