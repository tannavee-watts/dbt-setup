{{
    config(
        materialized='table',
        unique_key='order_id',
        dist='created_at',
        sort=['created_at']
    )
}}

WITH addresses AS (
    SELECT * FROM {{ ref ('addresses')}}
),

orders AS (
    SELECT * FROM {{ ref ('orders')}}
),

devices AS (
    SELECT 
        DISTINCT cast(d.type_id AS int64) AS order_id, 
        FIRST_VALUE(d.device) OVER (PARTITION BY d.type_id ORDER BY d.created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS device 
    FROM {{ ref ('devices')}} d 
    WHERE d.type = 'order' 
),

fulfilled_orders AS ( 
    SELECT 
        fo.user_id, 
        MIN(fo.order_id) AS first_order_id 
    FROM orders AS fo 
    WHERE fo.status != 'cancelled' 
    GROUP BY fo.user_id 
),

payments AS ( 
    SELECT 
        order_id, 
        SUM(CASE WHEN status = 'completed' THEN tax_amount_cents ELSE 0 END) AS gross_tax_amount_cents, 
        SUM(CASE WHEN status = 'completed' THEN amount_cents ELSE 0 END) AS gross_amount_cents, 
        SUM(CASE WHEN status = 'completed' THEN amount_shipping_cents ELSE 0 END) AS gross_shipping_amount_cents, 
        SUM(CASE WHEN status = 'completed' THEN tax_amount_cents + amount_cents + amount_shipping_cents ELSE 0 END) AS gross_total_amount_cents 
    FROM {{ ref ('payments')}}
    GROUP BY order_id 
),

order_history AS (
    SELECT 
        o.order_id, 
        o.user_id, 
        o.created_at, 
        o.updated_at, 
        o.shipped_at, 
        o.currency, 
        o.status AS order_status, 
        CASE WHEN o.status IN ('paid', 'completed', 'shipped') THEN 'completed' ELSE o.status END AS order_status_category, 
        CASE WHEN oa.country_code IS NULL THEN 'Null country' WHEN oa.country_code = 'US' THEN 'US' 
                WHEN oa.country_code != 'US' THEN 'International'END AS country_type, 
        o.shipping_method, 
        CASE WHEN d.device = 'web' THEN 'desktop' 
                WHEN d.device IN ('ios-app', 'android-app') THEN 'mobile-app'
                WHEN d.device IN ('mobile', 'tablet') THEN 'mobile-web'
                WHEN NULLIF(d.device, '') IS NULL THEN 'unknown' ELSE 'ERROR' END AS purchase_device_type, 
        d.device AS purchase_device, 
        CASE WHEN fo.first_order_id = o.order_id THEN 'new' ELSE 'repeat' END AS user_type,
        o.amount_total_cents, 
        pa.gross_total_amount_cents, 
        CASE WHEN o.currency = 'USD' then o.amount_total_cents ELSE pa.gross_total_amount_cents END AS total_amount_cents, 
        pa.gross_tax_amount_cents, 
        pa.gross_amount_cents, 
        pa.gross_shipping_amount_cents 
    FROM orders o 
    LEFT JOIN devices d ON d.order_id = o.order_id
    LEFT JOIN fulfilled_orders fo ON o.user_id = fo.user_id 
    LEFT JOIN addresses oa ON oa.order_id = o.order_id 
    LEFT JOIN payments pa ON pa.order_id = o.order_id 
)

SELECT 
    order_id,
    user_id,
    created_at,
    updated_at,
    shipped_at,
    currency,
    order_status,
    order_status_category,
    country_type,
    shipping_method,
    purchase_device_type,
    purchase_device,
    user_type,
    amount_total_cents,
    gross_total_amount_cents,
    total_amount_cents,
    gross_tax_amount_cents,
    gross_amount_cents,
    gross_shipping_amount_cents,
    amount_total_cents/100 AS amount_total, 
    gross_total_amount_cents/100 AS gross_total_amount, total_amount_cents/100 AS total_amount, 
    gross_tax_amount_cents/100 AS gross_tax_amount, gross_amount_cents/100 AS gross_amount, 
    gross_shipping_amount_cents/100 AS gross_shipping_amount
FROM order_history