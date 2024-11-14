{{ config(
    materialized='incremental',
    unique_key='requirement_id',
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT DISTINCT
        requirement_id,
        requirement_name,
        CURRENT_TIMESTAMP() as created_at
    FROM {{ source('staging', 'stg_requirements') }}
)

-- Use MERGE to insert new records and update existing ones
MERGE INTO {{ this }} AS target
USING source AS source
ON target.requirement_name = source.requirement_name

-- Insert new records if they don't exist in the target table
WHEN NOT MATCHED THEN
  INSERT (requirement_id, requirement_name, created_at)
  VALUES (source.requirement_id, source.requirement_name, source.created_at);
