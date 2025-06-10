# streamlit_app.py

import os
import sqlite3

import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st
from dotenv import load_dotenv

# ──────────────────────────────────────────────────────────────────────────────
# 1. Load environment variables and connect to SQLite
# ──────────────────────────────────────────────────────────────────────────────

load_dotenv()
DB_URL = os.getenv("DB_URL", "sqlite:///weather.db")

# If using SQLite, strip off "sqlite:///" to get the file path
if DB_URL.startswith("sqlite:///"):
    db_path = DB_URL.replace("sqlite:///", "")
else:
    db_path = DB_URL

@st.cache_data
def load_data():
    conn = sqlite3.connect(db_path)
    df = pd.read_sql(
        """
        SELECT
            fw.obs_ts,
            fw.temp_c,
            fw.feels_like_c,
            fw.humidity_pct,
            fw.pressure_hpa,
            fw.wind_speed_ms,
            fw.clouds_pct,
            dl.city_name,
            fw.weather_main,
            fw.weather_desc
        FROM fact_weather AS fw
        JOIN dim_location AS dl
          ON fw.location_id = dl.location_id
        """,
        conn,
    )
    conn.close()
    # Convert obs_ts → datetime (UTC-aware) and set as index
    df["obs_ts"] = pd.to_datetime(df["obs_ts"], utc=True, errors="coerce")
    return df.set_index("obs_ts")

df_weather = load_data()

# ──────────────────────────────────────────────────────────────────────────────
# 2. Sidebar controls
# ──────────────────────────────────────────────────────────────────────────────

st.sidebar.title("Filters & Options")
st.sidebar.markdown("Use the controls below to customize the dashboard view.")

# City filter
cities = df_weather["city_name"].unique().tolist()
selected_cities = st.sidebar.multiselect(
    "Select City (all by default)",
    options=cities,
    default=cities,
)

# Date range filter
min_date = df_weather.index.min().date()
max_date = df_weather.index.max().date()
start_date, end_date = st.sidebar.date_input(
    "Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

# Choose sections to display
st.sidebar.markdown("---")
section_overview = st.sidebar.checkbox("Show Overview", value=True)
section_stats = st.sidebar.checkbox("Show Statistics", value=False)
section_corr = st.sidebar.checkbox("Show Correlation", value=False)
section_viz = st.sidebar.checkbox("Show Visualizations", value=False)

# ──────────────────────────────────────────────────────────────────────────────
# 3. Filter data based on selections
# ──────────────────────────────────────────────────────────────────────────────

# Filter by city
if selected_cities:
    df_filtered = df_weather[df_weather["city_name"].isin(selected_cities)]
else:
    df_filtered = df_weather.copy()

# Convert start_date and end_date (Python date) to UTC-aware Timestamps
start_ts = pd.to_datetime(start_date).tz_localize("UTC")
end_ts = pd.to_datetime(end_date).tz_localize("UTC")

# Now filter by timestamp range
df_filtered = df_filtered.loc[
    (df_filtered.index >= start_ts) & (df_filtered.index <= end_ts)
]

# ──────────────────────────────────────────────────────────────────────────────
# 4. Page Title and Key Metrics
# ──────────────────────────────────────────────────────────────────────────────

st.title("🌦️ Weather Data Dashboard")
st.markdown(
    f"**Cities:** {', '.join(selected_cities) if selected_cities else 'All'}  \n"
    f"**Date Range:** {start_date} to {end_date}  \n"
    f"**Total Records:** {len(df_filtered)}"
)

if section_overview:
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(
            label="Total Records",
            value=len(df_filtered),
        )
    with col2:
        avg_temp = df_filtered["temp_c"].mean() if not df_filtered.empty else 0
        st.metric(
            label="Avg Temperature (°C)",
            value=f"{avg_temp:.1f}",
        )
    with col3:
        avg_humidity = df_filtered["humidity_pct"].mean() if not df_filtered.empty else 0
        st.metric(
            label="Avg Humidity (%)",
            value=f"{avg_humidity:.1f}",
        )

# ──────────────────────────────────────────────────────────────────────────────
# 5. Statistics Section
# ──────────────────────────────────────────────────────────────────────────────

if section_stats:
    st.header("📊 Descriptive Statistics by City")
    if df_filtered.empty:
        st.info("No data available for the selected filters.")
    else:
        stats_by_city = (
            df_filtered.groupby("city_name")
            .agg(
                {
                    "temp_c": ["mean", "min", "max", "std"],
                    "feels_like_c": ["mean", "min", "max", "std"],
                    "humidity_pct": ["mean", "min", "max", "std"],
                    "pressure_hpa": ["mean", "min", "max", "std"],
                    "wind_speed_ms": ["mean", "min", "max", "std"],
                    "clouds_pct": ["mean", "min", "max", "std"],
                }
            )
        )
        stats_by_city.columns = [
            "_".join(col).strip() for col in stats_by_city.columns.values
        ]
        with st.expander("Show Statistics Table"):
            st.dataframe(stats_by_city.style.format("{:.2f}"))

# ──────────────────────────────────────────────────────────────────────────────
# 6. Correlation Section
# ──────────────────────────────────────────────────────────────────────────────

if section_corr:
    st.header("🔗 Correlation Matrix")
    if df_filtered.empty:
        st.info("No data available for the selected filters.")
    else:
        numeric_cols = [
            "temp_c",
            "feels_like_c",
            "humidity_pct",
            "pressure_hpa",
            "wind_speed_ms",
            "clouds_pct",
        ]
        corr_matrix = df_filtered[numeric_cols].corr()
        fig, ax = plt.subplots(figsize=(6, 5))
        cax = ax.matshow(corr_matrix, cmap="coolwarm")
        fig.colorbar(cax)
        ax.set_xticks(range(len(numeric_cols)))
        ax.set_yticks(range(len(numeric_cols)))
        ax.set_xticklabels(numeric_cols, rotation=45, ha="left")
        ax.set_yticklabels(numeric_cols)
        st.pyplot(fig)

# ──────────────────────────────────────────────────────────────────────────────
# 7. Visualizations Section (Using Tabs)
# ──────────────────────────────────────────────────────────────────────────────

if section_viz:
    st.header("📈 Visualizations")

    tabs = st.tabs(["Histogram", "Boxplot", "Time Series", "Scatterplot", "Pie Chart"])

    # --- Tab 1: Histograms ---
    with tabs[0]:
        st.subheader("Histograms of Numeric Features")
        if df_filtered.empty:
            st.info("No data available for the selected filters.")
        else:
            numeric_cols = [
                "temp_c",
                "feels_like_c",
                "humidity_pct",
                "pressure_hpa",
                "wind_speed_ms",
                "clouds_pct",
            ]
            cols = st.columns(3)
            for i, col in enumerate(numeric_cols):
                fig, ax = plt.subplots()
                df_filtered[col].hist(bins=15, ax=ax)
                ax.set_title(f"{col}")
                ax.set_xlabel(col)
                ax.set_ylabel("Frequency")
                cols[i % 3].pyplot(fig)

    # --- Tab 2: Boxplots ---
    with tabs[1]:
        st.subheader("Boxplots by City")
        if df_filtered.empty:
            st.info("No data available for the selected filters.")
        else:
            fig1, ax1 = plt.subplots(figsize=(8, 4))
            df_filtered.boxplot(column="temp_c", by="city_name", ax=ax1)
            ax1.set_title("Temperature by City")
            ax1.set_xlabel("City")
            ax1.set_ylabel("Temperature (°C)")
            plt.suptitle("")
            st.pyplot(fig1)

            fig2, ax2 = plt.subplots(figsize=(8, 4))
            df_filtered.boxplot(column="humidity_pct", by="city_name", ax=ax2)
            ax2.set_title("Humidity by City")
            ax2.set_xlabel("City")
            ax2.set_ylabel("Humidity (%)")
            plt.suptitle("")
            st.pyplot(fig2)

    # --- Tab 3: Time Series ---
    with tabs[2]:
        st.subheader("Hourly Temperature Trend")
        if df_filtered.empty:
            st.info("No data available for the selected filters.")
        else:
            fig_ts, ax_ts = plt.subplots(figsize=(8, 4))
            for city, group in df_filtered.groupby("city_name"):
                hourly_mean = group["temp_c"].resample("H").mean()
                ax_ts.plot(hourly_mean.index.hour, hourly_mean.values, marker="o", label=city)
            ax_ts.set_title("Hourly Temperature by City (UTC Hour)")
            ax_ts.set_xlabel("Hour of Day (UTC)")
            ax_ts.set_ylabel("Temperature (°C)")
            ax_ts.set_xticks(range(0, 24))
            ax_ts.legend()
            st.pyplot(fig_ts)

    # --- Tab 4: Scatterplot ---
    with tabs[3]:
        st.subheader("Temperature vs. Humidity by City")
        if df_filtered.empty:
            st.info("No data available for the selected filters.")
        else:
            fig_sc, ax_sc = plt.subplots(figsize=(6, 4))
            colors = {"Bengaluru": "blue", "London": "green", "New York": "red"}
            for city in df_filtered["city_name"].unique():
                subset = df_filtered[df_filtered["city_name"] == city]
                ax_sc.scatter(
                    subset["temp_c"],
                    subset["humidity_pct"],
                    label=city,
                    alpha=0.7,
                    c=colors.get(city, "gray"),
                )
            ax_sc.set_title("Temp vs. Humidity")
            ax_sc.set_xlabel("Temperature (°C)")
            ax_sc.set_ylabel("Humidity (%)")
            ax_sc.legend()
            st.pyplot(fig_sc)

    # --- Tab 5: Pie Chart ---
    with tabs[4]:
        st.subheader("Weather Main Category Distribution")
        if df_filtered.empty:
            st.info("No data available for the selected filters.")
        else:
            weather_counts = df_filtered["weather_main"].value_counts()
            fig_pie, ax_pie = plt.subplots(figsize=(5, 5))
            weather_counts.plot(
                kind="pie", autopct="%1.1f%%", startangle=140, ax=ax_pie
            )
            ax_pie.set_ylabel("")
            ax_pie.set_title("Weather Categories")
            st.pyplot(fig_pie)
