import streamlit as st
import pandas as pd
import plotly.express as px

from queries import (
    get_fire_events,
    get_fire_kpis,
    get_daily_fire_kpis,
)


st.set_page_config(
    page_title="Smart Wildfire Intelligence",
    page_icon="🔥",
    layout="wide",
)


# ---------------------------------------------------------
# Page Header
# ---------------------------------------------------------

st.title("🔥 Smart Wildfire Intelligence System")
st.caption("Real-time wildfire monitoring and analytics dashboard")


# ---------------------------------------------------------
# Load Data
# ---------------------------------------------------------

try:
    kpis = get_fire_kpis()
    fire_events = get_fire_events(10000)
    daily_kpis = get_daily_fire_kpis()

except Exception as e:
    st.error(f"Database error: {e}")
    st.stop()


# ---------------------------------------------------------
# Sidebar
# ---------------------------------------------------------

st.sidebar.header("Dashboard Filters")

if not fire_events.empty:
    min_date = fire_events["event_timestamp"].min().date()
    max_date = fire_events["event_timestamp"].max().date()

    selected_date = st.sidebar.date_input(
        "Event date",
        value=max_date,
        min_value=min_date,
        max_value=max_date,
    )

    filtered_events = fire_events[
        fire_events["event_timestamp"].dt.date == selected_date
    ]
else:
    filtered_events = fire_events


# ---------------------------------------------------------
# KPI Cards
# ---------------------------------------------------------

st.subheader("🔥 Fire KPIs")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Total Fires",
        f"{int(kpis['total_fires']):,}"
    )

with col2:
    st.metric(
        "Total FRP",
        f"{kpis['total_frp']:,.0f}"
    )

with col3:
    st.metric(
        "Average FRP",
        f"{kpis['avg_frp']:.2f}"
    )

with col4:
    st.metric(
        "Night Fires",
        f"{int(kpis['night_fires']):,}"
    )


# ---------------------------------------------------------
# Daily Fire Analytics
# ---------------------------------------------------------

st.divider()
st.subheader("📊 Fire Activity")

if daily_kpis.empty:
    st.info("No daily fire KPI data available.")
else:
    daily_kpis["event_date"] = pd.to_datetime(
        daily_kpis["event_date"]
    )

    fig = px.bar(
        daily_kpis,
        x="event_date",
        y="fire_count",
        title="Fire Activity by Observed Date",
        labels={
            "event_date": "Observed Date",
            "fire_count": "Fire Count",
        },
        text="fire_count",
    )

    fig.update_traces(
        texttemplate="%{text:,}",
        textposition="outside",
    )

    fig.update_layout(
        xaxis=dict(
            type="category",
            tickangle=-45,
        ),
        yaxis=dict(
            title="Number of Fires",
            rangemode="tozero",
        ),
        height=450,
        margin=dict(
            l=20,
            r=20,
            t=60,
            b=100,
        ),
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
    )



# ---------------------------------------------------------
# Fire Map
# ---------------------------------------------------------

st.divider()
st.subheader("🗺️ Interactive Fire Map")

if filtered_events.empty:
    st.info("No fire events found for the selected date.")
else:
    map_data = filtered_events[
        [
            "latitude",
            "longitude",
            "frp",
            "brightness",
            "confidence",
            "event_timestamp",
        ]
    ].copy()

    map_data = map_data.dropna(
        subset=["latitude", "longitude"]
    )

    fig_map = px.scatter_map(
        map_data,
        lat="latitude",
        lon="longitude",
        size="frp",
        color="frp",
        hover_data=[
            "event_timestamp",
            "frp",
            "brightness",
            "confidence",
        ],
        zoom=3,
        height=650,
        title="Wildfire Events",
    )

    fig_map.update_layout(
        map_style="open-street-map",
        margin={
            "r": 0,
            "t": 40,
            "l": 0,
            "b": 0,
        },
    )

    st.plotly_chart(
        fig_map,
        use_container_width=True,
    )


# ---------------------------------------------------------
# Recent Fire Events
# ---------------------------------------------------------

st.divider()
st.subheader("📋 Recent Fire Events")

display_columns = [
    "event_timestamp",
    "latitude",
    "longitude",
    "frp",
    "brightness",
    "confidence",
    "daynight",
    "satellite",
]

available_columns = [
    col for col in display_columns
    if col in filtered_events.columns
]

st.dataframe(
    filtered_events[available_columns],
    use_container_width=True,
    hide_index=True,
)
