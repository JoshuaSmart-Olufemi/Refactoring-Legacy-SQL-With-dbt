WITH paid_orders as (

    select 
    orders.id as order_id,
    orders.user_id	as customer_id,
    orders.order_date AS order_placed_at,
    orders.STATUS AS order_status,
    payments.total_amount_paid,
    payments.payment_finalized_date,
    customers.FIRST_NAME as customer_first_name,
    customers.LAST_NAME as customer_last_name
FROM {{ ref('stg_jaffle_shop_orders')}} as orders
left join {{ ref('stg_stripe_payments')}} as payments ON orders.id = payments.order_id
left join {{ ref('stg_jaffle_shop_customers')}} as customers on orders.user_id = customers.id )

select * from paid_orders