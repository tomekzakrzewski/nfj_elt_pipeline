{{
  config(
    materialized='incremental',
    unique_key='company_id'
  )
}}

WITH source AS (
    SELECT DISTINCT
        company_name,
        city
    FROM {{ source('staging', 'stg_jobs') }}
),
final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['company_name', 'city']) }} as company_id,
        company_name,
        city,
        CURRENT_TIMESTAMP() as created_at
    FROM source
)

SELECT * FROM final
{% if is_incremental() %}
WHERE company_id NOT IN (SELECT company_id FROM {{ this }})
{% endif %}
