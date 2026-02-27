{{
  config(
    materialized='incremental',
    unique_key='event_id'
  )
}}

WITH menu_events AS (
    SELECT * FROM {{ source('main', 'raw_menu_events_v4') }}
    
    {% if is_incremental() %}
    WHERE timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
    {% endif %}
),

categories_unnested AS (
    SELECT
        event_id,
        restaurant_id,
        timestamp::timestamp as event_timestamp,
        unnest(
            cast(json_extract(menu_json, '$.categories') as JSON[])
        ) as category
    FROM menu_events
),

items_unnested AS (
    SELECT
        event_id,
        restaurant_id,
        event_timestamp,
        json_extract_string(category, '$.category_name') as category_name,
        unnest(
            cast(json_extract(category, '$.items') as JSON[])
        ) as item
    FROM categories_unnested
)

SELECT
    event_id,
    restaurant_id,
    event_timestamp,
    category_name,
    cast(json_extract(item, '$.item_id') as integer) as item_id,
    json_extract_string(item, '$.name') as item_name,
    cast(json_extract(item, '$.price') as decimal(10,2)) as price,
    json_extract_string(item, '$.description') as description,
    json_extract_string(item, '$.image_url') as image_url
FROM items_unnested
