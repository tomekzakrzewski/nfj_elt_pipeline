{{ config(
    materialized='incremental',
    unique_key='category_id',
    incremental_strategy='merge'
) }}

WITH source_data AS (
    SELECT
        category,
        MAX(scraped_at) AS scraped_at
    FROM {{ source('staging', 'stg_jobs') }}
    GROUP BY category
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['category']) }} AS category_id,
    category AS category_name,
    scraped_at
FROM source_data

{% if is_incremental() %}
WHERE scraped_at > (SELECT COALESCE(MAX(scraped_at), '1970-01-01') FROM {{ this }})
{% endif %}
