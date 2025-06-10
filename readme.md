
# Weather ETL & BI Pipeline

## Project Overview
This repository contains a complete end-to-end ETL (Extract, Transform, Load) pipeline that:
1. Fetches current and historical (back‐dated) weather data from the OpenWeatherMap API.
2. Stores the data in a SQLite database (`weather.db`), using two tables:
   - **dim_location**: city metadata (city name, country, latitude, longitude)
   - **fact_weather**: weather observations (timestamp, temperature, humidity, etc.)
3. Automates hourly updates and error‐notification via SMTP.
4. Provides an interactive Streamlit dashboard to explore and visualize the data.
5. (Optional) Demonstrates a simple ML model for humidity prediction, containerization with Docker, and future enhancements.

---

## Prerequisites
- Python 3.8 or later
- pip (Python package installer)
- A valid OpenWeatherMap API key
- (For email alerts) SMTP credentials capable of sending outbound emails
- (Optional) Docker, if you want to build and run a container

---

## Installation

1. **Clone the repository**  
   ```bash
   git clone https://github.com/your-username/weather-etl-pipeline.git
   cd weather-etl-pipeline
````

2. **Create and activate a virtual environment**

   ```bash
   python -m venv venv
   # On Windows (PowerShell):
   .\venv\Scripts\Activate
   # On macOS/Linux:
   source venv/bin/activate
   ```

3. **Install dependencies**

   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

   The main dependencies are:

   * `pandas`
   * `requests`
   * `SQLAlchemy`
   * `python-dotenv`
   * `schedule`
   * `streamlit`
   * `python-pptx` (for PPT generation)
   * `scikit-learn` (for the example ML model)

---

## Environment Variables

Create a file named `.env` in the project root with the following keys:

```
# 1. OpenWeatherMap API credentials
OWM_API_KEY=your_openweathermap_api_key

# 2. Database URL (using SQLite in this example)
DB_URL=sqlite:///weather.db

# 3. SMTP / Email settings (for failure notifications)
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
EMAIL_SENDER=your.email@gmail.com
EMAIL_PASSWORD=your_email_app_password
EMAIL_RECIPIENT=recipient@example.com
```

* `OWM_API_KEY`: Your OpenWeatherMap API key (free tier is sufficient).
* `DB_URL`: A SQLAlchemy‐style URL. For SQLite, use `sqlite:///weather.db`.
* `SMTP_*`: Credentials for a mailbox that can send an email. We recommend creating an App Password if using Gmail.

---

## Database & Tables

By default, the pipeline uses an on‐disk SQLite database named `weather.db` in the project folder. The schema is:

1. **dim\_location**

   * `location_id` (INT, primary key, autoincrement)
   * `city_name` (STRING)
   * `country` (STRING)
   * `lat` (NUMERIC)
   * `lon` (NUMERIC)

2. **fact\_weather**

   * `id` (INT, primary key, autoincrement)
   * `location_id` (INT, foreign key → dim\_location.location\_id)
   * `obs_ts` (DATETIME WITH TIMEZONE)
   * `temp_c` (NUMERIC)
   * `feels_like_c` (NUMERIC)
   * `humidity_pct` (NUMERIC)
   * `pressure_hpa` (NUMERIC)
   * `wind_speed_ms` (NUMERIC)
   * `weather_main` (STRING)
   * `weather_desc` (STRING)
   * `clouds_pct` (NUMERIC)

All tables are created (or recreated) by the Python script at runtime.

---

## ETL Script (`app.py`)

The main ETL logic lives in `app.py` (or your chosen Python module). Its responsibilities:

1. **recreate\_tables()**

   * Drops the existing `fact_weather` and `dim_location` tables (if they exist)
   * Recreates them from scratch, according to the SQLAlchemy metadata definitions.

2. **seed\_locations()**

   * Inserts a predefined list of cities (latitude/longitude + city name + country code) into `dim_location`.
   * Uses `INSERT OR IGNORE` so that rerunning does not duplicate.

3. **insert\_backdated\_snapshots(hours=25)**

   * Reads all cities from `dim_location`.
   * For each city, fetches one current weather record from the OpenWeatherMap “current weather” endpoint.
   * Generates N = 25 hourly snapshots by subtracting 0…24 hours from the base UNIX timestamp.
   * Converts those UNIX timestamps to UTC‐aware `datetime` objects and bulk‐inserts into `fact_weather`.

4. **etl\_once()**

   * Reads all rows from `dim_location`.
   * For each city, calls the API again to get a single, live‐data snapshot.
   * Converts JSON fields and UNIX timestamp → UTC `datetime`.
   * Inserts that one row into `fact_weather`.
   * If any exception occurs, triggers an SMTP email alert.

5. **schedule\_etl(interval\_minutes=60)** (optional)

   * Uses `schedule.every(interval_minutes).minutes.do(etl_once)` to run `etl_once()` every hour.
   * Call `schedule_etl()` instead of `etl_once()` if you want ongoing, automated updates.

6. **Main Entry Point**
   When you run `python app.py`, by default it:

   1. Calls `recreate_tables()`
   2. Calls `seed_locations()`
   3. Calls `insert_backdated_snapshots(hours=25)`
   4. Calls `etl_once()` once (live data)
   5. Exits (unless you uncomment the `schedule_etl(60)` line to keep it running)

---

## Running the ETL

1. Ensure your virtual environment is active and `.env` is configured.
2. Run the script:

   ```bash
   python app.py
   ```
3. On first run, all tables are dropped and recreated, 25 snapshots per city are inserted, then a current snapshot is inserted.
4. On subsequent runs, comment out (or remove) the `recreate_tables()` and `seed_locations()` calls in `__main__` to avoid wiping data. Just run:

   ```bash
   python app.py
   ```

   to insert a fresh “current” snapshot.
5. For continuous hourly execution, uncomment:

   ```python
   # schedule_etl(interval_minutes=60)
   ```

   in `app.py`, then run:

   ```bash
   python app.py
   ```

   The script will stay alive, run `etl_once()` every hour, and send you an email if an exception is thrown.

---

## Streamlit Dashboard (`streamlit_app.py`)

A separate script, `streamlit_app.py`, reads from `weather.db` and provides interactive charts:

1. **Sidebar Controls**

   * Multi‐select box for filtering by city
   * Date‐range picker to restrict data in the chosen window
   * Checkboxes to toggle sections: Overview, Statistics, Correlation, Visualizations

2. **Overview Metrics**

   * Total records in the filtered subset
   * Average temperature (°C)
   * Average humidity (%)

3. **Statistics Section**

   * Grouped aggregation by `city_name`
   * Shows mean / min / max / std for all numeric columns: `temp_c`, `feels_like_c`, `humidity_pct`, `pressure_hpa`, `wind_speed_ms`, `clouds_pct`

4. **Correlation Matrix**

   * Heatmap of pairwise correlations among numeric features

5. **Visualizations (Tabs)**

   * **Histogram**: Distribution of each numeric variable
   * **Boxplot**: Temperature and Humidity distributions by city
   * **Time Series**: Hourly temperature trend (UTC-based) per city
   * **Scatterplot**: Temperature vs. Humidity, colored by city
   * **Pie Chart**: Proportion of `weather_main` categories (e.g., Clear, Clouds, Rain)

### Running the Dashboard

1. Activate your virtual environment.
2. Install Streamlit if necessary:

   ```bash
   pip install streamlit
   ```
3. Launch the app from project root:

   ```bash
   streamlit run streamlit_app.py
   ```

   or, if `streamlit` is not on your PATH:

   ```bash
   python -m streamlit run streamlit_app.py
   ```
4. A browser window/tab will open at `http://localhost:8501` (default). Use the sidebar filters to explore your weather data.

---

## Example ML Model (Optional)

Under `ml_model_example.py` (or within a notebook), a simple linear regression is trained to predict humidity from:

* `temp_c`
* `feels_like_c`

**Steps:**

1. Pull data from `fact_weather`, group or sample so each city has ≥ 2 records (to avoid R² issues).
2. Split into train/test (80/20).
3. Fit `sklearn.linear_model.LinearRegression()`.
4. Evaluate MSE and R².
5. Output learned coefficients and metrics.

You can adapt this to more advanced forecasting (ARIMA, LSTM) in the future.

---

## Docker Container (Optional)

A sample `Dockerfile` is included for packaging the ETL script:

```Dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV OWM_API_KEY=<your_api_key> \
    DB_URL=sqlite:///weather.db \
    SMTP_SERVER=smtp.gmail.com \
    SMTP_PORT=587 \
    EMAIL_SENDER=your.email@gmail.com \
    EMAIL_PASSWORD=your_app_password \
    EMAIL_RECIPIENT=notify@example.com

CMD ["python", "app.py"]
```

**Build & Run:**

```bash
docker build -t weather-etl-pipeline .
docker run -d --name weather_etl weather-etl-pipeline
```

This will run `python app.py` inside the container. Since we're using SQLite, you may wish to mount a volume so data persists:

```bash
docker run -d \
  -v "$(pwd)/weather.db:/app/weather.db" \
  --name weather_etl \
  weather-etl-pipeline
```

---

## File Structure

```
weather-etl-pipeline/
├── app.py
├── streamlit_app.py
├── ml_model_example.py          # (optional)
├── requirements.txt
├── README.md
├── .env                         # (not committed; contains secret keys)
├── weather.db                   # SQLite database (created at runtime)
├── Dockerfile                   # (optional containerization)
└── docs/
    └── weather_etl_detailed_dashboard.pptx
    └── weather_etl_architecture_matter.pptx
```

* `app.py`  — Main ETL script that recreates tables, seeds cities, backdates data, and does live ETL.
* `streamlit_app.py` — Streamlit dashboard source.
* `ml_model_example.py` — Example of training a linear regression model on the weather data.
* `requirements.txt` — All Python dependencies.
* `Dockerfile` — Optional Docker container definition.
* `weather.db` — SQLite database generated by `app.py`.
* `README.md` — (this file).
* `/docs` — Contains PPTX slides: architecture and detailed breakdown.

---

## Next Steps & Enhancements

* Support dynamic city input or read from an external CSV/list.
* Migrate to PostgreSQL or MySQL for production-grade scalability.
* Implement more advanced time‐series forecasting models (ARIMA, LSTM).
* Add alerts (Slack, SMS) on extreme weather thresholds.
* Deploy Streamlit dashboard to a cloud platform (Streamlit Cloud, Heroku, AWS).
* Improve UI/UX with Plotly or custom CSS.

---
