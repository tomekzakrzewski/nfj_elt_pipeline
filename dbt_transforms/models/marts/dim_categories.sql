{{
  config(
    materialized='incremental',
    unique_key='category_id'
  )
}}

WITH source AS (
    SELECT DISTINCT
        category
    FROM {{ source('staging', 'stg_jobs') }}
),
final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['category']) }} as category_id,
        category as category_name,
        CURRENT_TIMESTAMP() as created_at
    FROM source
)

SELECT * FROM final
{% if is_incremental() %}
WHERE category_id NOT IN (SELECT category_id FROM {{ this }})
{% endif %}
