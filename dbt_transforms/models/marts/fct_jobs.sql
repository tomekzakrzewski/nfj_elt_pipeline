{{
  config(
    materialized='incremental',
    unique_key='job_id'
  )
}}

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
)

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
