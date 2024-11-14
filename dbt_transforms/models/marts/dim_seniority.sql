{{ config(
    materialized='incremental',
    unique_key='seniority_id',
    incremental_strategy='merge'
) }}

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

-- Use MERGE to insert new records and avoid duplicates
MERGE INTO {{ this }} AS target
USING final AS source
ON target.seniority_level = source.seniority_level

-- Insert new records if they don't exist in the target table
WHEN NOT MATCHED THEN
  INSERT (seniority_id, seniority_level)
  VALUES (source.seniority_id, source.seniority_level);
