WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'INVOICE2') }}
),

cleaned AS (
    SELECT
        ID AS invoice_line_id,
        TRIM(INVNO) AS invoice_no,
        TRIM(ITEMCODE) AS item_code,
        TRIM(BARCODENO) AS barcode_no,
        UPPER(TRIM(ITEMNAME)) AS item_name,
        QTY AS quantity,
        UNITPRICE AS unit_price,
        DISCOUNT AS discount,
        AMOUNT AS amount,
        TRY_TO_DATE(DATE) AS invoice_date,
        LAST_UPDATED AS last_updated
    FROM source
)

SELECT *
FROM cleaned
WHERE invoice_no IS NOT NULL