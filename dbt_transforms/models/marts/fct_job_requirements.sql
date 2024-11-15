{{ config(
    materialized='incremental',
    unique_key=['job_id', 'requirement_id'],
    incremental_strategy='merge'
) }}

WITH source_data AS (
    SELECT
        job_id,
        requirement_id,
        MAX(scraped_at) AS scraped_at
    FROM {{ source('staging', 'stg_job_requirements') }}
    GROUP BY job_id, requirement_id
)

SELECT
    job_id,
    requirement_id,
    scraped_at
FROM source_data

{% if is_incremental() %}
WHERE scraped_at > (SELECT COALESCE(MAX(scraped_at), '1970-01-01') FROM {{ this }})
{% endif %}
