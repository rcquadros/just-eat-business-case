{{
  config(
    materialized='table'
  )
}}

WITH items_with_changes AS (
    SELECT * FROM {{ ref('int_menu_items_with_changes') }}
),

items_filtered AS (
    SELECT *
    FROM items_with_changes
    WHERE has_changes = TRUE
),

items_scd2 AS (
    SELECT
        restaurant_id || '-' || item_id || '-' || event_timestamp::varchar as menu_item_key,
        
        restaurant_id,
        item_id,
        item_name,
        category_name,
        price,
        description,
        image_url,
        
        event_timestamp as valid_from,
        

        CASE 
            WHEN LEAD(event_timestamp) OVER (
                PARTITION BY restaurant_id, item_id 
                ORDER BY event_timestamp
            ) IS NOT NULL 
            THEN LEAD(event_timestamp) OVER (
                PARTITION BY restaurant_id, item_id 
                ORDER BY event_timestamp
            ) - INTERVAL '1 second'
            ELSE NULL
        END as valid_to,
        
        CASE 
            WHEN LEAD(event_timestamp) OVER (
                PARTITION BY restaurant_id, item_id 
                ORDER BY event_timestamp
            ) IS NULL THEN TRUE
            ELSE FALSE
        END as is_current,

        event_id,
        sequence_number,
        CURRENT_TIMESTAMP as dbt_updated_at
        
    FROM items_filtered
)

SELECT * FROM items_scd2
