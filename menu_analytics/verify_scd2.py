import duckdb

con = duckdb.connect('C:/Users/RodrigoConteQuadros/just_eat_case_study/just_eat.duckdb', read_only=True)

print("=" * 100)
print("SCD2 VERIFICATION - Sample Item History")
print("=" * 100)

result = con.execute("""
    SELECT 
        menu_item_key,
        restaurant_id,
        item_id,
        item_name,
        price,
        valid_from,
        valid_to,
        is_current,
        sequence_number
    FROM int_menu_items_scd2
    WHERE restaurant_id = 5001 
    AND item_id = 101  -- Classic Burger
    ORDER BY valid_from
""").df()

print(result.to_string(index=False))
print()

print("=" * 100)
print("SCD2 SUMMARY STATS")
print("=" * 100)

summary = con.execute("""
    SELECT 
        COUNT(*) as total_scd2_records,
        COUNT(DISTINCT restaurant_id || '-' || item_id) as unique_items,
        SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_records,
        SUM(CASE WHEN is_current = FALSE THEN 1 ELSE 0 END) as historical_records,
        COUNT(DISTINCT restaurant_id) as num_restaurants
    FROM int_menu_items_scd2
""").df()

print(summary.to_string(index=False))

con.close()
