{{
  config(
    materialized='table'
  )
}}

WITH current_menus AS (
    SELECT * 
    FROM {{ ref('int_menu_items_scd2') }}
    WHERE is_current = TRUE
),

restaurants AS (
    SELECT * FROM {{ ref('dim_restaurants') }}
)

SELECT
    r.restaurant_id,
    r.country_code,
    r.cuisine_type,
    r.partner_tier,
    r.signup_date,
    
    COUNT(m.item_id) as menu_size,
    
    ROUND(
        100.0 * SUM(CASE 
            WHEN m.description IS NOT NULL 
            AND m.image_url IS NOT NULL 
            THEN 1 ELSE 0 
        END) / NULLIF(COUNT(m.item_id), 0),
        2
    ) as menu_completeness_score,
    
    SUM(CASE WHEN m.category_name = 'Main Dishes' THEN 1 ELSE 0 END) as main_dishes_count,
    SUM(CASE WHEN m.category_name = 'Sides' THEN 1 ELSE 0 END) as sides_count,
    SUM(CASE WHEN m.category_name NOT IN ('Main Dishes', 'Sides') THEN 1 ELSE 0 END) as other_category_count,
    
    ROUND(AVG(m.price), 2) as avg_price,
    ROUND(MIN(m.price), 2) as min_price,
    ROUND(MAX(m.price), 2) as max_price,
    
    SUM(CASE WHEN m.description IS NULL THEN 1 ELSE 0 END) as items_missing_description,
    SUM(CASE WHEN m.image_url IS NULL THEN 1 ELSE 0 END) as items_missing_image,
    
    CURRENT_TIMESTAMP as last_updated

FROM restaurants r
LEFT JOIN current_menus m ON r.restaurant_id = m.restaurant_id
GROUP BY 
    r.restaurant_id,
    r.country_code,
    r.cuisine_type,
    r.partner_tier,
    r.signup_date
