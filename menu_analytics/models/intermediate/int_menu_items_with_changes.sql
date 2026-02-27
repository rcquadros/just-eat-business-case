{{
  config(
    materialized='incremental',
    unique_key=['event_id', 'restaurant_id', 'item_id']
  )
}}

WITH menu_items AS (
    SELECT * FROM {{ ref('stg_menu_items') }}
    
    {% if is_incremental() %}
    -- Only process new events
    WHERE event_id NOT IN (SELECT DISTINCT event_id FROM {{ this }})
    {% endif %}
),

items_with_sequence AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY restaurant_id, item_id 
            ORDER BY event_timestamp
        ) as sequence_number
    FROM menu_items
),

items_with_changes AS (
    SELECT
        current.*,
        LAG(current.price) OVER (
            PARTITION BY current.restaurant_id, current.item_id 
            ORDER BY current.event_timestamp
        ) as prev_price,
        LAG(current.description) OVER (
            PARTITION BY current.restaurant_id, current.item_id 
            ORDER BY current.event_timestamp
        ) as prev_description,
        LAG(current.image_url) OVER (
            PARTITION BY current.restaurant_id, current.item_id 
            ORDER BY current.event_timestamp
        ) as prev_image_url,
        LAG(current.item_name) OVER (
            PARTITION BY current.restaurant_id, current.item_id 
            ORDER BY current.event_timestamp
        ) as prev_item_name,
        LAG(current.category_name) OVER (
            PARTITION BY current.restaurant_id, current.item_id 
            ORDER BY current.event_timestamp
        ) as prev_category_name
    FROM items_with_sequence current
)

SELECT
    event_id,
    restaurant_id,
    item_id,
    item_name,
    category_name,
    price,
    description,
    image_url,
    event_timestamp,
    sequence_number,
    CASE 
        WHEN sequence_number = 1 THEN TRUE
        WHEN price != prev_price THEN TRUE
        WHEN COALESCE(description, '') != COALESCE(prev_description, '') THEN TRUE
        WHEN COALESCE(image_url, '') != COALESCE(prev_image_url, '') THEN TRUE
        WHEN item_name != prev_item_name THEN TRUE
        WHEN category_name != prev_category_name THEN TRUE
        ELSE FALSE
    END as has_changes
FROM items_with_changes
