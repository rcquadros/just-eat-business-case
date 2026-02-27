import duckdb

con = duckdb.connect('C:/Users/RodrigoConteQuadros/just_eat_case_study/just_eat.duckdb', read_only=True)

print("=" * 120)
print("MENU METRICS BY RESTAURANT")
print("=" * 120)

result = con.execute("""
    SELECT 
        restaurant_id,
        country_code,
        cuisine_type,
        partner_tier,
        menu_size,
        menu_completeness_score,
        main_dishes_count,
        sides_count,
        other_category_count,
        avg_price,
        min_price,
        max_price,
        items_missing_description,
        items_missing_image
    FROM mart_menu_metrics
    ORDER BY restaurant_id
""").df()

print(result.to_string(index=False))

con.close()
