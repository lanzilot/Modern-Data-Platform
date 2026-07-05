WITH customer AS (
    SELECT *
    FROM {{ ref('stg_customer') }}
),

deduped AS (
    SELECT *
    FROM customer
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY customer_code
        ORDER BY last_updated DESC, customer_id DESC
    ) = 1
)

SELECT
    customer_id,
    customer_code,
    customer_name,
    address,
    last_updated
FROM deduped