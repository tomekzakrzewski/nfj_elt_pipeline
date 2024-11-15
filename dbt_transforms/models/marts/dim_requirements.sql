{{ config(
    materialized='incremental',
    unique_key='requirement_id',
    incremental_strategy='merge'
) }}

WITH source_data AS (
    SELECT
        requirement_id,
        requirement_name,
        scraped_at,
        ROW_NUMBER() OVER (PARTITION BY requirement_id ORDER BY scraped_at DESC) AS row_num
    FROM {{ source('staging', 'stg_requirements') }}
)

SELECT
    requirement_id,
    requirement_name,
    scraped_at
FROM source_data
WHERE row_num = 1

{% if is_incremental() %}
AND scraped_at > (SELECT COALESCE(MAX(scraped_at), '1970-01-01') FROM {{ this }})
{% endif %}
