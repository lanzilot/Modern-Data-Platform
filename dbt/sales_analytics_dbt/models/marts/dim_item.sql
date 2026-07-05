WITH items AS (
    SELECT
        item_code,
        MAX(barcode_no) AS barcode_no,
        MAX(item_name) AS item_name
    FROM {{ ref('stg_invoice2') }}
    WHERE item_code IS NOT NULL
    GROUP BY item_code
)

SELECT
    ROW_NUMBER() OVER (ORDER BY item_code) AS item_key,
    item_code,
    barcode_no,
    item_name
FROM items