{{
  config(
    materialized='incremental',
    unique_key='seniority_id'
  )
}}

WITH source AS (
    SELECT DISTINCT
        seniority
    FROM {{ source('staging', 'stg_jobs') }}
),
final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['seniority']) }} AS seniority_id,
        seniority AS seniority_level
    FROM source
)

SELECT * FROM final
{% if is_incremental() %}
    WHERE seniority_id NOT IN (SELECT seniority_id FROM {{ this }})
{% endif %}
