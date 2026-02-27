{{
  config(
    materialized='table'
  )
}}

SELECT
    restaurant_id,
    country_code,
    cuisine_type,
    partner_tier,
    signup_date::date as signup_date
FROM {{ source('main', 'dim_restaurants_v3') }}