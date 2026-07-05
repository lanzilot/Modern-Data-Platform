WITH source AS (
    SELECT *
    FROM {{ source('bronze', 'CUSTOMER') }}
),

cleaned AS (
    SELECT
        ID AS customer_id,
        TRIM(CUSTCODE) AS customer_code,
        UPPER(TRIM(CUSTNAME)) AS customer_name,
        TRIM(ADDRESS) AS address,
        LAST_UPDATED AS last_updated
    FROM source
)

SELECT *
FROM cleaned
WHERE customer_code IS NOT NULL