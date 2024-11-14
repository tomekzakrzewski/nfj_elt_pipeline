{{
  config(
    materialized='incremental',
    unique_key='company_id',
    incremental_strategy='merge'
  )
}}

WITH source AS (
    SELECT DISTINCT
        company_name,
        scraped_at
    FROM {{ source('staging', 'stg_jobs') }}
),
final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['company_name']) }} as company_id,
        company_name,
        scraped_at
    FROM source
)

SELECT *
FROM final

{% if is_incremental() %}
-- Filter out records that already exist in the target table based on seniority_id
WHERE company_name NOT IN (SELECT company_name FROM {{ this }})
{% endif %}
