{{ config(
    materialized='incremental',
    unique_key='requirement_id',
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT DISTINCT
        requirement_id,
        requirement_name,
        scraped_at
    FROM {{ source('staging', 'stg_requirements') }}
)

SELECT *
FROM source

{% if is_incremental() %}
-- Filter out records that already exist in the target table based on seniority_id
WHERE requirement_name NOT IN (SELECT requirement_name FROM {{ this }})
{% endif %}
