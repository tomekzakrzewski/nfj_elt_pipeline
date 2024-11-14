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
        city
    FROM {{ source('staging', 'stg_jobs') }}
),
final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['company_name']) }} as company_id,
        company_name,
        city,
        CURRENT_TIMESTAMP() as created_at
    FROM source
)

-- Use MERGE to handle both inserts and updates
MERGE INTO {{ this }} AS target
USING final AS source
ON target.company_id = source.company_id

WHEN NOT MATCHED THEN
  INSERT (company_id, company_name, city, created_at)
  VALUES (source.company_id, source.company_name, source.city, source.created_at);
