WITH paid_orders as (

    select * from {{ ref('paid_orders') }}
-----------------------------------------------------------------
),

customer_orders as (

    select * from {{ ref('int_customer_orders') }}

),
------------------------------------------------------------------------
final as (

select
    paid_orders.*,
    row_number() over (order by paid_orders.order_id) as transaction_seq,
    row_number() over (partition by paid_orders.customer_id order by paid_orders.order_id) as customer_sales_seq,
    case when customer_orders.first_order_date = paid_orders.order_placed_at
    then 'new customer'
    else 'returned customer' 
    end as new_and_returned_customers,
    sum(paid_orders.total_amount_paid) over (partition by paid_orders.customer_id order by paid_orders.order_placed_at) as clv,
    customer_orders.first_order_date as first_date_of_sale
from paid_orders 
left join customer_orders on paid_orders.customer_id = customer_orders.customer_id
order by order_id 
)

select * from final 
