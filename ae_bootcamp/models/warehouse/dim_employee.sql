WITH source AS (

    SELECT
        id as employee_id
        , company
        , last_name
        , first_name
        , email_address
        , job_title
        , business_phone
        , home_phone
        , mobile_phone
        , fax_number
        , address
        , city
        , state_province
        , zip_postal_code
        , country_region
        , web_page
        , notes
        , attachments
        , ingestion_timestamp
    FROM {{ ref('stg_employees') }}

),

unique_source as (
    SELECT 
        *
        , ROW_NUMBER() OVER (PARTITION BY employee_id) AS ROW_NUMBER
    FROM source
)

SELECT *
EXCEPT (row_number)
FROM unique_source
WHERE row_number = 1