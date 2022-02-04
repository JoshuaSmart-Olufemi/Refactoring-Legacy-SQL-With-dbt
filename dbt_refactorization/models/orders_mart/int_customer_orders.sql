customer_orders as (

    select customers.id as customer_id
    , min(order_date) as first_order_date
    , max(order_date) as most_recent_order_date
    , count(orders.id) AS number_of_orders
from {{ ref ('stg_jaffle_shop_customers')}} as customers
left join {{ ref('stg_jaffle_shop_orders') }} as orders
on orders.user_id = customers.id 
group by 1)

select * from customer_orders