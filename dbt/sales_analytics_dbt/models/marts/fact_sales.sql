WITH invoice_header AS (
    SELECT *
    FROM {{ ref('stg_invoice1') }}
),

invoice_lines AS (
    SELECT *
    FROM {{ ref('stg_invoice2') }}
),

customer AS (
    SELECT *
    FROM {{ ref('stg_customer') }}
),

item AS (
    SELECT *
    FROM {{ ref('dim_item') }}
)

SELECT
    l.invoice_line_id,
    h.invoice_id,
    h.invoice_no,
    h.invoice_date,
    c.customer_id,
    c.customer_code,
    i.item_key,
    l.item_code,
    l.quantity,
    l.unit_price,
    l.discount,
    l.amount,
    l.last_updated
FROM invoice_lines l
INNER JOIN invoice_header h
    ON l.invoice_no = h.invoice_no
LEFT JOIN {{ ref('dim_customer') }} c
    ON h.customer_code = c.customer_code
LEFT JOIN item i
    ON l.item_code = i.item_code