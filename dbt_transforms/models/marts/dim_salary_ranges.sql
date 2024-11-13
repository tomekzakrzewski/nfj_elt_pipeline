{{
  config(
    materialized='table',
    unique_key='salary_id'
  )
}}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['salary_from', 'salary_to', 'salary_type', 'salary_currency']) }} as salary_id,
    salary_from,
    salary_to,
    salary_type,
    salary_currency
FROM {{ source('staging', 'stg_jobs') }}
