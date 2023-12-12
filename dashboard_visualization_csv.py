import streamlit as st
import pandas as pd
import altair as alt
from datetime import datetime, timedelta

# Set the Streamlit title
st.title('Fuel Prices Visualization')

# Function to load data from a CSV file
@st.experimental_memo(ttl=300)  # Cache the data, and refresh every 5 minutes
def load_data():
    df = pd.read_csv('fuel_prices.csv')
    df['lastupdated'] = pd.to_datetime(df['lastupdated'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
    return df

# Main execution code
data = load_data()

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

# Set up a session state for last update time
if 'last_update' not in st.session_state:
    st.session_state['last_update'] = datetime.now()

# Check if the last update was more than 5 minutes ago, if so, rerun the app
def auto_refresh():
    if datetime.now() - st.session_state.last_update > timedelta(minutes=5):
        st.session_state.last_update = datetime.now()
        st.experimental_rerun()

auto_refresh()
