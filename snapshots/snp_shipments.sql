{% snapshot shipments_snapshot %}

{{  config 
    (
    target_database ='QWT_ANALYTICS_DEV',
    target_schema = 'SNAPSHOTS_DEV',
    unique_key = "ORDERID||'-'||LINENO",

    strategy='timestamp',
    updated_at='SHIPMENTDATE')
}}

select c.* from  {{ ref("stg_shipments") }} c

{% endsnapshot %}