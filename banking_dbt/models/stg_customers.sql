{{ config(materialized='view') }}

with ranked as (
  SELECT
    v:id::string             as customer_id,
    v:first_name::string     as first_name,
    v:last_name::string      as last_name,
    v:email::string          as email,
    v:created_at::timestamp  as created_at,
    v:ingested_at::timestamp as ingested_at,
    row_number() over (
      partition by v:id::string
      order by COALESCE(v:ingested_at::timestamp, '1970-01-01'::timestamp) desc
    ) as rn
  from {{ source('raw', 'customers') }}
)

SELECT
  customer_id,
  first_name,
  last_name,
  email,
  created_at,
  ingested_at
FROM ranked
WHERE rn = 1