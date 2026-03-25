{{ config (materialized='table') }}

with latest as (

  SELECT 
  account_id,
  customer_id,
  account_type,
  balance,
  currency,
  created_at,
  dbt_valid_from as effective_from,
  dbt_valid_to as effective_to,
  case when dbt_valid_to is null then true else false end as is_current

  from {{ref('accounts_snapshot')}}
)
select * from latest