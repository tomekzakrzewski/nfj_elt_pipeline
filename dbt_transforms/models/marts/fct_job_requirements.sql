{{ config(
    materialized='incremental',
    unique_key=['job_id', 'requirement_id'],
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT DISTINCT
        job_id,
        requirement_id,
        scraped_at
    FROM {{ source('staging', 'stg_job_requirements') }}
)

SELECT *
FROM source

{% if is_incremental() %}
-- Only include new or updated records since the last run.
WHERE scraped_at > (SELECT MAX(scraped_at) FROM {{ this }})
{% endif %}
