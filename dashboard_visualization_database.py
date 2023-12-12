import streamlit as st
from sqlalchemy import create_engine
import pandas as pd
import altair as alt

# Set the Streamlit title
st.title('Fuel Prices Visualization')

# Create a database connection
def create_connection():
    host = "localhost"
    user = "postgres"
    psw = "123456789"
    database = "postgres"
    return create_engine(f"postgresql+psycopg2://{user}:{psw}@{host}/{database}")

# Fetch data from the database
@st.cache_data
def load_data():
    conn = create_connection()
    query = "SELECT * FROM fuel_prices"
    return pd.read_sql(query, conn)

# Convert the 'lastupdated' column to datetime format for visualization
def convert_to_datetime(df):
    df['lastupdated'] = pd.to_datetime(df['lastupdated'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
    return df

# Main execution code
data = load_data()
data = convert_to_datetime(data)

# Display the table
st.write(data)

# Visualization using Altair
fuel_types = data['fueltype'].unique().tolist()
selected_fueltype = st.selectbox('Select a fuel type:', fuel_types)

filtered_data = data[data['fueltype'] == selected_fueltype]

chart = alt.Chart(filtered_data).mark_line().encode(
    x='lastupdated:T',
    y='price:Q',
    tooltip=['stationcode', 'price', 'lastupdated']
).properties(title=f'Price Trend for {selected_fueltype}')

st.altair_chart(chart, use_container_width=True)

