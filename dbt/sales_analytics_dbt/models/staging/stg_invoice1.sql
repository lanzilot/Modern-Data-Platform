WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'INVOICE1') }}
),

cleaned AS (
    SELECT
        ID AS invoice_id,
        TRIM(INVNO) AS invoice_no,
        TRIM(CUSTCODE) AS customer_code,
        TRY_TO_DATE(DATE) AS invoice_date,
        LAST_UPDATED AS last_updated
    FROM source
    WHERE INVNO IS NOT NULL
),

deduped AS (
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY invoice_no
        ORDER BY last_updated DESC, invoice_id DESC
    ) = 1
)

SELECT *
FROM deduped