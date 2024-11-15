{{ config(
    materialized='incremental',
    unique_key='company_id',
    incremental_strategy='merge'
) }}

WITH source_data AS (
    SELECT
        company_name,
        MAX(scraped_at) AS scraped_at
    FROM {{ source('staging', 'stg_jobs') }}
    GROUP BY company_name
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['company_name']) }} AS company_id,
    company_name,
    scraped_at
FROM source_data

{% if is_incremental() %}
WHERE scraped_at > (SELECT COALESCE(MAX(scraped_at), '1970-01-01') FROM {{ this }})
{% endif %}
