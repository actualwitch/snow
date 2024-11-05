from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import sum, col
import streamlit as st
import pandas as pd

# Write directly to the app
st.title("Warehouse dashboard")

# Get the current credentials
session = get_active_session()


@st.cache_data()
def load_data():
    # first, lets try getting the data from raw sql query
    as_sql = session.sql(
        """
            SELECT 
                truck_brand_name,
                menu_item_name,
                SUM(sale_price_usd - cost_of_goods_usd) as profit,
            FROM tasty_bytes_sample_data.raw_pos.menu
            GROUP BY truck_brand_name, menu_item_name
            ORDER BY truck_brand_name, profit DESC;
        """
    ).collect()
    # now lets do the same thing but using the DataFrame API
    as_df = (
        session.table("tasty_bytes_sample_data.raw_pos.menu")
        .select(
            col("truck_brand_name"),
            col("menu_item_name"),
            col("sale_price_usd"),
            col("cost_of_goods_usd"),
        )
        .group_by("truck_brand_name", "menu_item_name")
        .agg(sum(col("sale_price_usd") - col("cost_of_goods_usd")).name("profit"))
        .order_by("truck_brand_name", col("profit").desc())
        .collect()
    )

    return as_sql, as_df


@st.cache_data()
def load_inventory_data():
    inventory = session.table("tasty_bytes_sample_data.raw_pos.storage").collect()
    return inventory


data, data_as_df = load_data()
inventory_data = load_inventory_data()

# Display profit data
st.header("Profit by Menu Item")
data

# Display inventory data
st.header("Current Inventory by Brand")
inventory_df = pd.DataFrame(inventory_data)
for brand in inventory_df['TRUCK_BRAND_NAME'].unique():
    st.subheader(brand)
    brand_inventory = inventory_df[inventory_df['TRUCK_BRAND_NAME'] == brand]
    st.dataframe(
        brand_inventory[['INGREDIENT', 'QUANTITY', 'LAST_UPDATED']]
        .sort_values('QUANTITY', ascending=False)
    )
