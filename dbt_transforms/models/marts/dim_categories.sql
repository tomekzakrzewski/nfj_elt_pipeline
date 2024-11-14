{{
  config(
    materialized='incremental',
    unique_key='category_id'
    incremental_strategy='merge'
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

-- Use MERGE to handle both inserts and updates
MERGE INTO {{ this }} AS target
USING final AS source
ON target.category_name = source.category_name

WHEN NOT MATCHED THEN
  INSERT (category_id, category_name, created_at)
  VALUES (source.category_id, source.category_name, source.created_at);
