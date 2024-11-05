-- here's just an assortment of queries i came up with to analyze the data, to be used elsewhere in the app
-- select menu items and its ingredients
SELECT m.menu_id, m.menu_item_name, obj.value:"ingredients"::ARRAY AS ingredients FROM tasty_bytes_sample_data.raw_pos.menu m, LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj WHERE 1=1;

-- find most versatile ingredients (used in most menu items)
WITH flattened_ingredients AS (
  SELECT 
    m.menu_item_name,
    i.value as ingredient
  FROM tasty_bytes_sample_data.raw_pos.menu m,
  LATERAL FLATTEN(input => m.menu_item_health_metrics_obj:menu_item_health_metrics[0]:ingredients) i
)
SELECT 
  ingredient,
  COUNT(DISTINCT menu_item_name) as times_used,
  LISTAGG(menu_item_name, ', ') as used_in_items
FROM flattened_ingredients
GROUP BY ingredient
ORDER BY times_used DESC;

-- find most profitable menu items by truck brand
SELECT 
    truck_brand_name,
    menu_item_name,
    SUM(sale_price_usd - cost_of_goods_usd) as profit,
FROM tasty_bytes_sample_data.raw_pos.menu
GROUP BY truck_brand_name, menu_item_name
ORDER BY truck_brand_name, profit DESC;

-- analyze average cost of healthy vs non-healthy items
SELECT 
    obj.value:is_healthy_flag::STRING as is_healthy,
    COUNT(*) as item_count,
    AVG(cost_of_goods_usd) as avg_cost,
    AVG(sale_price_usd) as avg_price,
    AVG(sale_price_usd - cost_of_goods_usd) as avg_profit
FROM tasty_bytes_sample_data.raw_pos.menu m,
LATERAL FLATTEN (input => m.menu_item_health_metrics_obj:menu_item_health_metrics) obj
GROUP BY is_healthy
ORDER BY is_healthy;