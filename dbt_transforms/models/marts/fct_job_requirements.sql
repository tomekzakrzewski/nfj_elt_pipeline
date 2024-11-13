{{
  config(
    materialized='incremental',
    unique_key=['job_id', 'requirement_id']
  )
}}

SELECT DISTINCT
    job_id,
    requirement_id
FROM {{ source('staging', 'stg_job_requirements') }}
{% if is_incremental() %}
WHERE (job_id, requirement_id) NOT IN (
    SELECT job_id, requirement_id
    FROM {{ this }}
)
{% endif %}
