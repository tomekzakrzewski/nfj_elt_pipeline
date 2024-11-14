{{
  config(
    materialized='incremental',
    unique_key='category_id',
    incremental_strategy='merge'
  )
}}

WITH source AS (
    SELECT DISTINCT
        category,
        scraped_at
    FROM {{ source('staging', 'stg_jobs') }}
),
final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['category']) }} as category_id,
        category as category_name,
        scraped_at
    FROM source
)

SELECT *
FROM final

{% if is_incremental() %}
-- Filter out records that already exist in the target table based on seniority_id
WHERE category_name NOT IN (SELECT category_name FROM {{ this }})
{% endif %}
