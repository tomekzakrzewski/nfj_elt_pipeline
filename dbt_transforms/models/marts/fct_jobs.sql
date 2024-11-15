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
source_data AS (
    SELECT
        j.job_id,
        c.company_id,
        cat.category_id,
        s.salary_id,
        sen.seniority_id,
        j.title,
        j.reference,
        j.scraped_at,
        j.city,
        ROW_NUMBER() OVER (PARTITION BY j.job_id ORDER BY j.scraped_at DESC) AS row_num
    FROM {{ source('staging', 'stg_jobs') }} j
    LEFT JOIN companies c ON j.company_name = c.company_name
    LEFT JOIN categories cat ON j.category = cat.category_name
    LEFT JOIN seniority sen ON j.seniority = sen.seniority_level
    LEFT JOIN salary_ranges s ON j.salary_from = s.salary_from
        AND j.salary_to = s.salary_to
        AND j.salary_type = s.salary_type
        AND j.salary_currency = s.salary_currency
)

SELECT *
FROM source_data
WHERE row_num = 1

{% if is_incremental() %}
AND scraped_at > (SELECT COALESCE(MAX(scraped_at), '1970-01-01') FROM {{ this }})
{% endif %}
