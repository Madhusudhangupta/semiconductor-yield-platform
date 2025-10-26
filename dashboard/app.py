import streamlit as st
import pandas as pd
import altair as alt
import random

# --- IMPORTANT ---
# In a real app, you would connect to Snowflake here.
# from snowflake.connector import connect

# @st.cache_data
# def load_data_from_snowflake():
#     conn = connect(
#         user=st.secrets["SNOWFLAKE_USER"],
#         password=st.secrets["SNOWFLAKE_PASSWORD"],
#         account=st.secrets["SNOWFLAKE_ACCOUNT"],
#         warehouse="ANALYTICS_WH",
#         database="SEMICONDUCTOR_PROD",
#         schema="ANALYTICS"
#     )
#     query = """
#     SELECT 
#         y.dt,
#         y.final_yield_percentage,
#         y.failure_reason_code,
#         e.tool_type,
#         p.duration_sec,
#         p."avg_Pressure_Torr_avg"
#     FROM fact_wafer_yield y
#     JOIN fact_process_measurements p ON y.wafer_id = p.wafer_id
#     JOIN dim_equipment e ON p.equipment_id = e.equipment_id
#     WHERE p.step_name = 'Etch';
#     """
#     df = pd.read_sql(query, conn)
#     conn.close()
#     return df

def load_mock_data():
    """Generates mock data to run the dashboard without a DB connection."""
    dates = pd.date_range('2025-10-01', periods=30)
    data = []
    for date in dates:
        for tool_type in ['Etcher', 'Lithography']:
            yield_val = 90 + random.uniform(-5, 5) if tool_type == 'Etcher' else 95 + random.uniform(-2, 2)
            pressure = 50 + random.uniform(-5, 10) if tool_type == 'Etcher' else 20 + random.uniform(-1, 1)
            failure = 'None' if random.random() > 0.1 else 'Etch_Anomaly'
            data.append({
                'dt': date,
                'final_yield_percentage': yield_val,
                'failure_reason_code': failure,
                'tool_type': tool_type,
                'duration_sec': 180 + random.uniform(-10, 10),
                'avg_Pressure_Torr_avg': pressure
            })
    return pd.DataFrame(data)

# --- Streamlit App Layout ---
st.set_page_config(layout="wide")
st.title("Semiconductor Yield Analytics Dashboard 📈")

# --- Load Data ---
# In a real app, use this:
# df = load_data_from_snowflake()
# For this example, we use mock data:
df = load_mock_data()
df['dt'] = pd.to_datetime(df['dt'])

# --- Main Page KPIs ---
st.header("Overall Yield Performance")

# 1. Main KPI
avg_yield = df['final_yield_percentage'].mean()
st.metric("Average Wafer Yield", f"{avg_yield:.2f}%")

# 2. Yield Over Time
st.subheader("Yield % Over Time")
yield_chart = alt.Chart(df).mark_line(point=True).encode(
    x=alt.X('dt', title='Date'),
    y=alt.Y('final_yield_percentage', title='Yield %', scale=alt.Scale(zero=False)),
    color='tool_type',
    tooltip=['dt', 'final_yield_percentage', 'tool_type']
).interactive()
st.altair_chart(yield_chart, use_container_width=True)

# --- Process & Equipment Analysis ---
st.header("Process Analysis (Etch)")
col1, col2 = st.columns(2)

with col1:
    # 3. Process Parameter vs. Yield
    st.subheader("Etch Pressure vs. Yield")
    etch_df = df[df['tool_type'] == 'Etcher']
    scatter_chart = alt.Chart(etch_df).mark_circle(size=60).encode(
        x=alt.X('avg_Pressure_Torr_avg', title='Avg. Etch Pressure (Torr)'),
        y=alt.Y('final_yield_percentage', title='Yield %', scale=alt.Scale(zero=False)),
        color=alt.Color('failure_reason_code', title='Failure Reason'),
        tooltip=['avg_Pressure_Torr_avg', 'final_yield_percentage', 'failure_reason_code']
    ).interactive()
    st.altair_chart(scatter_chart, use_container_width=True)

with col2:
    # 4. Failure Reason Breakdown
    st.subheader("Failure Analysis")
    failure_df = df[df['failure_reason_code'] != 'None']['failure_reason_code'].value_counts().reset_index()
    failure_chart = alt.Chart(failure_df).mark_bar().encode(
        x=alt.X('count', title='Count'),
        y=alt.Y('failure_reason_code', title='Failure Reason')
    )
    st.altair_chart(failure_chart, use_container_width=True)