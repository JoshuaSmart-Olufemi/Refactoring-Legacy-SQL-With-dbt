{% set old_etl_relation=ref('customer_orders')%}
{% set dbt_relation=ref('customer_order_changes')%}

{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    exclude_columns=["loaded_at"],
    primary_key="order_id"
) }}