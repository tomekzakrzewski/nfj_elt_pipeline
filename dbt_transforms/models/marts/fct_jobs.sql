{{ config(
    materialized='incremental',
    unique_key='job_id',
    incremental_strategy='merge'
) }}

WITH companies AS (
    SELECT * FROM {{ ref('dim_companies') }}
),
categories AS (
    SELECT * FROM {{ ref('dim_categories') }}
),
salary_ranges AS (
    SELECT * FROM {{ ref('dim_salary_ranges') }}
),
seniority AS (
    SELECT * FROM {{ ref('dim_seniority') }}
),
final AS (
    SELECT DISTINCT
        j.job_id,
        c.company_id,
        cat.category_id,
        s.salary_id,
        sen.seniority_id,
        j.title,
        j.reference
    FROM {{ source('staging', 'stg_jobs') }} j
    LEFT JOIN companies c
        ON j.company_name = c.company_name
        AND j.city = c.city
    LEFT JOIN categories cat
        ON j.category = cat.category_name
    LEFT JOIN seniority sen
        ON j.seniority = sen.seniority_level
    LEFT JOIN salary_ranges s
        ON j.salary_from = s.salary_from
        AND j.salary_to = s.salary_to
        AND j.salary_type = s.salary_type
        AND j.salary_currency = s.salary_currency
)

-- Use MERGE to insert new records and avoid duplicates based on job_id and reference.
MERGE INTO {{ this }} AS target
USING final AS source
ON target.job_id = source.job_id

-- Insert new records if they don't exist in the target table.
WHEN NOT MATCHED THEN
  INSERT (job_id, company_id, category_id, salary_id, seniority_id, title, reference)
  VALUES (source.job_id, source.company_id, source.category_id, source.salary_id, source.seniority_id, source.title, source.reference);
