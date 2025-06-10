#!/usr/bin/env python3
"""
app.py

A complete ETL pipeline script that:
1. Recreates (drops & re-creates) SQLite tables.
2. Seeds dim_location with sample cities.
3. Inserts at least 25 back-dated hourly snapshots per city into fact_weather.
4. Runs a single ETL cycle for the current weather.
5. Sends email alerts if any step fails.
6. Optionally schedules the ETL to run periodically.

Requirements:
- A .env file in the same directory containing:
    OWM_API_KEY=your_openweathermap_key
    DB_URL=sqlite:///weather.db
    SMTP_SERVER=smtp.gmail.com
    SMTP_PORT=587
    EMAIL_SENDER=your.email@gmail.com
    EMAIL_PASSWORD=your_app_password
    EMAIL_RECIPIENT=notify.to@example.com

Usage:
    python app.py
"""

import os
import time
import smtplib
import sqlite3
from email.mime.text import MIMEText
from datetime import datetime

import pandas as pd
import requests
import schedule
from sqlalchemy import (
    create_engine,
    MetaData,
    Table,
    Column,
    Integer,
    String,
    Numeric,
    DateTime,
    ForeignKey,
    text,
)
from dotenv import load_dotenv

# ──────────────────────────────────────────────────────────────────────────────
# 1. Load environment variables
# ──────────────────────────────────────────────────────────────────────────────

load_dotenv()  # Read .env

API_KEY = os.getenv("OWM_API_KEY")
DB_URL   = os.getenv("DB_URL")

SMTP_SERVER     = os.getenv("SMTP_SERVER")
SMTP_PORT       = int(os.getenv("SMTP_PORT", 587))
EMAIL_SENDER    = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD  = os.getenv("EMAIL_PASSWORD")
EMAIL_RECIPIENT = os.getenv("EMAIL_RECIPIENT")

# Validate required env vars
if not API_KEY:
    raise RuntimeError("OWM_API_KEY not found in environment.")
if not DB_URL:
    raise RuntimeError("DB_URL not found in environment.")
if not (SMTP_SERVER and EMAIL_SENDER and EMAIL_PASSWORD and EMAIL_RECIPIENT):
    raise RuntimeError("SMTP configuration not fully set in environment.")

print("✅ Loaded API_KEY, DB_URL, and email settings.")

# ──────────────────────────────────────────────────────────────────────────────
# 2. Email alert helper
# ──────────────────────────────────────────────────────────────────────────────

def send_failure_alert(subject: str, body: str):
    """
    Send a plain-text email via SMTP when the ETL or other steps fail.
    """
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"]    = EMAIL_SENDER
    msg["To"]      = EMAIL_RECIPIENT

    try:
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        print("✅ Failure alert sent via email.")
    except Exception as e:
        print(f"❌ Failed to send alert email: {e}")

# ──────────────────────────────────────────────────────────────────────────────
# 3. Create SQLAlchemy engine and metadata
# ──────────────────────────────────────────────────────────────────────────────

engine = create_engine(DB_URL, echo=False)
meta   = MetaData()

# ──────────────────────────────────────────────────────────────────────────────
# 4. Define table schemas
# ──────────────────────────────────────────────────────────────────────────────

dim_location = Table(
    "dim_location",
    meta,
    Column("location_id", Integer, primary_key=True, autoincrement=True),
    Column("city_name",   String(100), nullable=False),
    Column("country",     String(50),  nullable=False),
    Column("lat",         Numeric,     nullable=False),
    Column("lon",         Numeric,     nullable=False),
)

fact_weather = Table(
    "fact_weather",
    meta,
    Column("id",            Integer, primary_key=True, autoincrement=True),
    Column("location_id",   Integer, ForeignKey("dim_location.location_id")),
    Column("obs_ts",        DateTime(timezone=True), nullable=False),
    Column("temp_c",        Numeric),
    Column("feels_like_c",  Numeric),
    Column("humidity_pct",  Numeric),
    Column("pressure_hpa",  Numeric),
    Column("wind_speed_ms", Numeric),
    Column("weather_main",  String(50)),
    Column("weather_desc",  String(100)),
    Column("clouds_pct",    Numeric),
)

# ──────────────────────────────────────────────────────────────────────────────
# 5. Create (or recreate) the tables
# ──────────────────────────────────────────────────────────────────────────────

def recreate_tables():
    """
    Drop existing tables if they exist, then create them fresh.
    WARNING: This deletes all data in dim_location and fact_weather.
    """
    try:
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS fact_weather"))
            conn.execute(text("DROP TABLE IF EXISTS dim_location"))
        meta.create_all(engine)
        print("✅ Tables dropped (if they existed) and recreated.")
    except Exception as e:
        subject = "ETL Failure Alert: recreate_tables"
        body = f"An error occurred while recreating tables:\n\n{e}"
        send_failure_alert(subject, body)
        raise

# ──────────────────────────────────────────────────────────────────────────────
# 6. Seed dim_location with sample cities
# ──────────────────────────────────────────────────────────────────────────────

def seed_locations():
    """
    Insert a predefined list of cities into dim_location.
    Uses SQLite's INSERT OR IGNORE so rerunning won't duplicate.
    """
    try:
        cities = [
            (12.9716,  77.5946, "Bengaluru", "IN"),
            (51.5072,  -0.1276, "London",    "GB"),
            (40.7128, -74.0060, "New York",  "US"),
        ]
        with engine.begin() as conn:
            for lat, lon, city, country in cities:
                conn.execute(
                    text("""
                        INSERT OR IGNORE INTO dim_location (lat, lon, city_name, country)
                        VALUES (:lat, :lon, :city, :country)
                    """),
                    {"lat": lat, "lon": lon, "city": city, "country": country},
                )
        df_locs = pd.read_sql("SELECT * FROM dim_location", engine)
        print("✅ Seeded dim_location:")
        print(df_locs.to_string(index=False))
    except Exception as e:
        subject = "ETL Failure Alert: seed_locations"
        body = f"An error occurred while seeding dim_location:\n\n{e}"
        send_failure_alert(subject, body)
        raise

# ──────────────────────────────────────────────────────────────────────────────
# 7. Helper: fetch one “current” weather record per city
# ──────────────────────────────────────────────────────────────────────────────

OWM_URL = "https://api.openweathermap.org/data/2.5/weather"

def fetch_one_record(location_row: dict) -> dict:
    """
    Given a dict with keys (location_id, lat, lon),
    call /data/2.5/weather once and return a flattened dict including UNIX obs_ts.
    """
    params = {
        "lat":  location_row["lat"],
        "lon":  location_row["lon"],
        "units":"metric",
        "appid":API_KEY
    }
    r = requests.get(OWM_URL, params=params, timeout=10)
    r.raise_for_status()
    data = r.json()
    return {
        "location_id":    location_row["location_id"],
        "obs_ts":         data["dt"],              # UNIX timestamp (seconds)
        "temp_c":         data["main"]["temp"],
        "feels_like_c":   data["main"]["feels_like"],
        "humidity_pct":   data["main"]["humidity"],
        "pressure_hpa":   data["main"]["pressure"],
        "wind_speed_ms":  data["wind"]["speed"],
        "clouds_pct":     data["clouds"]["all"],
        "weather_main":   data["weather"][0]["main"],
        "weather_desc":   data["weather"][0]["description"],
    }

# ──────────────────────────────────────────────────────────────────────────────
# 8. Back-date helper: insert at least 25 hourly snapshots per city
# ──────────────────────────────────────────────────────────────────────────────

def insert_backdated_snapshots(hours: int = 25):
    """
    For each city in dim_location, fetch one current weather record,
    then duplicate it with obs_ts shifted back for `hours` consecutive hours.
    Inserts all snapshots into fact_weather in one batch.
    """
    try:
        # Load all city rows into a list of dicts
        conn_sql = sqlite3.connect("weather.db")
        df_locs = pd.read_sql("SELECT location_id, lat, lon FROM dim_location", conn_sql)
        conn_sql.close()

        if df_locs.empty:
            print("⚠️ No cities found in dim_location; skipping back-date.")
            return

        all_rows = []
        for row in df_locs.itertuples(index=False):
            loc_dict = {
                "location_id": row.location_id,
                "lat":          row.lat,
                "lon":          row.lon
            }
            base = fetch_one_record(loc_dict)
            # Generate `hours` snapshots: subtract 0..(hours-1) hours
            for h in range(hours):
                snapshot = base.copy()
                snapshot["obs_ts"] = base["obs_ts"] - (h * 3600)
                all_rows.append(snapshot)
            time.sleep(1)  # throttle API calls

        # Convert to DataFrame and transform obs_ts to UTC datetime
        df_insert = pd.DataFrame(all_rows)
        df_insert["obs_ts"] = pd.to_datetime(df_insert["obs_ts"], unit="s", utc=True)

        # Select columns in the correct order
        df_to_write = df_insert[[
            "location_id", "obs_ts", "temp_c", "feels_like_c",
            "humidity_pct", "pressure_hpa", "wind_speed_ms",
            "weather_main", "weather_desc", "clouds_pct"
        ]]

        # Bulk-insert into fact_weather
        conn_sql = sqlite3.connect("weather.db")
        df_to_write.to_sql("fact_weather", conn_sql, if_exists="append", index=False)
        conn_sql.close()

        print(f"✅ Inserted {len(df_to_write)} back-dated rows "
              f"({hours} hourly snapshots per city).")
    except Exception as e:
        subject = "ETL Failure Alert: insert_backdated_snapshots"
        body = f"An error occurred in back-date helper:\n\n{e}"
        send_failure_alert(subject, body)
        raise

# ──────────────────────────────────────────────────────────────────────────────
# 9. Define standard ETL: fetch current weather and insert one row per city
# ──────────────────────────────────────────────────────────────────────────────

def etl_once():
    """
    1. Read all rows from dim_location.
    2. For each, fetch current weather.
    3. Convert UNIX obs_ts → UTC datetime, build DataFrame.
    4. Insert into fact_weather.
    """
    try:
        df_locs = pd.read_sql("SELECT location_id, lat, lon FROM dim_location", engine)
        if df_locs.empty:
            print("⚠️ No cities in dim_location; aborting ETL.")
            return

        rows = []
        for row in df_locs.itertuples(index=False):
            loc_dict = {
                "location_id": row.location_id,
                "lat":          row.lat,
                "lon":          row.lon
            }
            try:
                flat = fetch_one_record(loc_dict)
                rows.append(flat)
                time.sleep(1)  # throttle API calls
            except Exception as fetch_err:
                print(f"❌ Fetch error for location_id={row.location_id}: {fetch_err}")

        if not rows:
            print("⚠️ No data fetched; skipping insert.")
            return

        df = pd.DataFrame(rows)
        df["obs_ts"] = pd.to_datetime(df["obs_ts"], unit="s", utc=True)

        df_to_insert = df[[
            "location_id", "obs_ts", "temp_c", "feels_like_c",
            "humidity_pct", "pressure_hpa", "wind_speed_ms",
            "weather_main", "weather_desc", "clouds_pct"
        ]]

        with engine.begin() as conn:
            df_to_insert.to_sql("fact_weather", conn, if_exists="append", index=False)

        print(f"✅ Loaded {len(df_to_insert)} row(s) at {datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC")
    except Exception as e:
        subject = "ETL Failure Alert: etl_once"
        body = f"An exception occurred during ETL:\n\n{e}"
        send_failure_alert(subject, body)
        raise

# ──────────────────────────────────────────────────────────────────────────────
# 10. (Optional) Schedule ETL to run periodically
# ──────────────────────────────────────────────────────────────────────────────

def schedule_etl(interval_minutes: int = 60):
    """
    Schedule the `etl_once` function to run every `interval_minutes`.
    """
    schedule.clear()
    schedule.every(interval_minutes).minutes.do(etl_once)
    print(f"⏰ Scheduled ETL every {interval_minutes} minute(s).")

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Scheduler stopped by user.")

# ──────────────────────────────────────────────────────────────────────────────
# 11. Main entry point
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        # Step 1: Recreate tables (uncomment if forcing a full refresh)
        recreate_tables()

        # Step 2: Seed dim_location with sample cities
        seed_locations()

        # Step 3: Insert at least 25 back-dated hourly snapshots per city
        insert_backdated_snapshots(hours=25)

        # Step 4: Run a single ETL cycle (inserts one current snapshot per city)
        etl_once()

        # Step 5: (Optional) Schedule ETL every hour
        # To enable scheduling, uncomment the line below:
        # schedule_etl(interval_minutes=60)

        print("🟢 app.py has finished its initial run. Exiting.")
    except Exception as main_err:
        subject = "ETL Failure Alert: main"
        body = f"An exception occurred in the main section:\n\n{main_err}"
        send_failure_alert(subject, body)
        raise
