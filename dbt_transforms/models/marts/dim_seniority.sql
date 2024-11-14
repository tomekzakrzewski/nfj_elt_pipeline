{{ config(
    materialized='incremental',
    unique_key='seniority_id',
    incremental_strategy='merge'
) }}

WITH source AS (
    -- Select distinct seniority levels from the staging table
    SELECT DISTINCT
        seniority,
        scraped_at
    FROM {{ source('staging', 'stg_jobs') }}
),

final AS (
    -- Generate a surrogate key for each distinct seniority level
    SELECT
        {{ dbt_utils.generate_surrogate_key(['seniority']) }} AS seniority_id,
        seniority AS seniority_level,
        scraped_at
    FROM source
)

SELECT *
FROM final

{% if is_incremental() %}
-- Filter out records that already exist in the target table based on seniority_id
WHERE seniority_level NOT IN (SELECT seniority_level FROM {{ this }})
{% endif %}
