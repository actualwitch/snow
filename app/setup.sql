CREATE OR ALTER VERSIONED SCHEMA core;

-- minimal setup to get some test data
USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
CREATE OR REPLACE DATABASE tasty_bytes_sample_data;
CREATE OR REPLACE SCHEMA tasty_bytes_sample_data.raw_pos;
-- lets use the default tasty bytes data
CREATE OR REPLACE TABLE tasty_bytes_sample_data.raw_pos.menu
(
    menu_id NUMBER(19,0),
    menu_type_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38,4),
    sale_price_usd NUMBER(38,4),
    menu_item_health_metrics_obj VARIANT
);
/*
  menu object looks like
  {
    "menu_item_health_metrics": [
      {
        "ingredients": [
          "Lemons",
          "Sugar",
          "Water"
        ],
        "is_dairy_free_flag": "Y",
        "is_gluten_free_flag": "Y",
        "is_healthy_flag": "N",
        "is_nut_free_flag": "Y"
      }
    ],
    "menu_item_id": 10
  }
 */

CREATE OR REPLACE STAGE tasty_bytes_sample_data.public.blob_stage
url = 's3://sfquickstarts/tastybytes/'
file_format = (type = csv);
COPY INTO tasty_bytes_sample_data.raw_pos.menu
FROM @tasty_bytes_sample_data.public.blob_stage/raw_pos/menu/;

CREATE OR REPLACE TABLE tasty_bytes_sample_data.raw_pos.storage_events
(
    event_id NUMBER AUTOINCREMENT,
    truck_brand_name VARCHAR(16777216),
    ingredient VARCHAR(16777216),
    quantity_delta NUMBER(38,2),  -- positive for additions, negative for deductions
    event_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (event_id)
);

-- create view for current inventory state
CREATE OR REPLACE VIEW tasty_bytes_sample_data.raw_pos.storage AS
SELECT 
    truck_brand_name,
    ingredient,
    SUM(quantity_delta) as quantity,
    MAX(event_timestamp) as last_updated
FROM tasty_bytes_sample_data.raw_pos.storage_events
GROUP BY truck_brand_name, ingredient
HAVING quantity > 0;

-- seed initial stocking events based on previous balance
INSERT INTO tasty_bytes_sample_data.raw_pos.storage_events 
    (truck_brand_name, ingredient, quantity_delta)
WITH unique_brand_ingredients AS (
  SELECT DISTINCT
    m.truck_brand_name,
    i.value::STRING as ingredient
  FROM tasty_bytes_sample_data.raw_pos.menu m,
  LATERAL FLATTEN(input => m.menu_item_health_metrics_obj:menu_item_health_metrics[0]:ingredients) i
)
SELECT 
    truck_brand_name,
    ingredient,
    100 as quantity_delta -- ought to do it
FROM unique_brand_ingredients;
