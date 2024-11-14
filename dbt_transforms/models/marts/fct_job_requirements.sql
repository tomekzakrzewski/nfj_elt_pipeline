{{ config(
    materialized='incremental',
    unique_key=['job_id', 'requirement_id'],
    incremental_strategy='merge'
) }}

WITH source AS (
    SELECT DISTINCT
        job_id,
        requirement_id
    FROM {{ source('staging', 'stg_job_requirements') }}
)

-- Use MERGE to insert new records and avoid duplicates
MERGE INTO {{ this }} AS target
USING source AS source
ON target.job_id = source.job_id
AND target.requirement_id = source.requirement_id

-- Insert new records if they don't exist in the target table
WHEN NOT MATCHED THEN
  INSERT (job_id, requirement_id)
  VALUES (source.job_id, source.requirement_id);
