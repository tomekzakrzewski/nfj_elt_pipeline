{{ config(
    materialized='incremental',
    unique_key='salary_id',
    incremental_strategy='merge'
) }}

WITH source_data AS (
    SELECT
        salary_from,
        salary_to,
        salary_type,
        salary_currency,
        MAX(scraped_at) AS scraped_at
    FROM {{ source('staging', 'stg_jobs') }}
    GROUP BY salary_from, salary_to, salary_type, salary_currency
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['salary_from', 'salary_to', 'salary_type', 'salary_currency']) }} AS salary_id,
    salary_from,
    salary_to,
    salary_type,
    salary_currency,
    scraped_at
FROM source_data

{% if is_incremental() %}
WHERE scraped_at > (SELECT COALESCE(MAX(scraped_at), '1970-01-01') FROM {{ this }})
{% endif %}
