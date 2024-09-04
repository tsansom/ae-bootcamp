WITH source AS (
    SELECT
        p.id AS product_id
        , p.product_code
        , p.product_name
        , p.description
        , s.company as supplier_company
        , p.standard_cost
        , p.list_price
        , p.reorder_level
        , p.target_level
        , p.quantity_per_unit
        , p.discontinued
        , p.minimum_reorder_quantity
        , p.category
        , p.attachments
        , CURRENT_TIMESTAMP() AS ingestion_timestamp
    FROM {{ ref('stg_products') }} AS p
    LEFT JOIN {{ ref('stg_suppliers') }} AS s
        ON s.id = p.supplier_id
)

, unique_source as (
    SELECT 
        *
        , ROW_NUMBER() OVER (PARTITION BY product_id) AS ROW_NUMBER
    FROM source
)

SELECT *
EXCEPT (row_number)
FROM unique_source
WHERE row_number = 1