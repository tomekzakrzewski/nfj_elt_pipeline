{{
  config(
    materialized='incremental',
    unique_key='requirement_id'
  )
}}

SELECT
    requirement_id,
    requirement_name,
    CURRENT_TIMESTAMP() as created_at
FROM {{ source('staging', 'stg_requirements') }}
{% if is_incremental() %}
WHERE requirement_id NOT IN (SELECT requirement_id FROM {{ this }})
{% endif %}
