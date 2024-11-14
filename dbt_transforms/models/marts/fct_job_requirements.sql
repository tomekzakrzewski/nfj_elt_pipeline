{{
  config(
    materialized='incremental',
    unique_key=['job_id', 'requirement_id']
  )
}}

SELECT DISTINCT
    new_data.job_id,
    new_data.requirement_id
FROM {{ source('staging', 'stg_job_requirements') }} AS new_data
{% if is_incremental() %}
LEFT JOIN {{ this }} AS existing_data
ON new_data.job_id = existing_data.job_id
AND new_data.requirement_id = existing_data.requirement_id
WHERE existing_data.job_id IS NULL  -- Only select rows that don't exist in target
{% endif %}
