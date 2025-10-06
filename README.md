# Nepal Real-Time Weather Monitoring & Early Warning System

![Nepal Weather System Banner](https://via.placeholder.com/800x200/007BFF/FFFFFF?text=Nepal+Weather+Monitoring+System)  


## Overview

This project implements a **real-time weather monitoring and early warning system** tailored for Nepal, focusing on flood and landslide risks during monsoon seasons. It uses live weather data from external APIs, streams it via Apache Kafka, processes it with distributed computing (Dask), caches alerts in Redis, and displays a live console dashboard for quick insights.

### Key Features
- **Live Data Ingestion**: Fetches real-time weather from [WeatherAPI.com](https://www.weatherapi.com/) for major Nepali stations (e.g., Kathmandu, Pokhara).
- **Streaming Pipeline**: Publishes/subscribes to Kafka for decoupled, scalable data flow.
- **Risk Assessment**: Calculates flood (based on precipitation/river levels) and landslide (hills-specific, soil moisture) risks with configurable thresholds.
- **Distributed Processing**: Uses Dask for parallel batch analytics; falls back to Pandas.
- **Alert Storage**: Redis for fast caching of high-risk alerts (with TTL and recent lists).
- **Live Dashboard**: Console-based UI with tables, stats, and active alerts; refreshes every 5 seconds.
- **Fallbacks**: Simulated data if APIs/Kafka fail; optional big data tools (runs with basics).
- **Nepal-Specific**: Monsoon adjustments (June–Sept), region-based thresholds (valleys, hills, plains).

The system is modular: Run the data feeder separately for production, or use the consumer's built-in simulator for testing.

### Architecture
```
[WeatherAPI.com] → [new_live_data_feeder.py (Kafka Producer)] → [Kafka Topic: 'nepal-weather']
                                                                 ↓
[nepal_weather_system.py (Kafka Consumer)] → [Dask Processing] → [Risk Calcs] → [Redis Alerts]
                                                                 ↓
[Live Console Dashboard] ← [Multi-Threaded Updates]
```
- **Throughput**: ~8 stations every 30s (feeder); processes batches of 50+.
- **Scalability**: Kafka/Dask ready for clusters; add ML for advanced forecasts.

## Prerequisites

- **Python 3.8+** (tested on 3.12).
- **Services** (optional, with fallbacks):
  - [Apache Kafka](https://kafka.apache.org/) (v3.0+): Local setup via Docker or brew.
  - [Redis](https://redis.io/) (v7+): Local server.
  - [Dask](https://dask.org/): For distributed processing.
- **API Key**: Free from [WeatherAPI.com](https://www.weatherapi.com/) for live data.

## Installation

1. **Clone/Setup Project**:
   ```bash
   git clone https://github.com/sujanach-rya/real_time_weather_detection nepal-weather-system
   cd nepal-weather-system
   ```

2. **Virtual Environment** (recommended):
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/macOS
   # or venv\Scripts\activate on Windows
   ```

3. **Install Dependencies**:
   Core (always):
   ```bash
   pip install requests kafka-python pandas numpy
   ```
   Optional (big data):
   ```bash
   pip install dask[complete] redis
   ```
   Full one-liner:
   ```bash
   pip install requests kafka-python dask[complete] redis pandas numpy
   ```

4. **Set Environment Variables**:
   - For live data: `export WEATHER_API_KEY=your_key_from_weatherapi.com`

5. **Start Services** (if using):
   - **Kafka**: Use Docker:
     ```bash
     docker run -p 9092:9092 apache/kafka:latest
     ```
     Or download from [kafka.apache.org/quickstart](https://kafka.apache.org/quickstart).
   - **Redis**: `redis-server` (or Docker: `docker run -p 6379:6379 redis`).

## Usage

### 1. Run the Data Feeder (Producer)
This streams live/simulated data to Kafka. Run in one terminal.

```bash
python new_live_data_feeder.py
```

- Output: "🚀 Starting... 📡 Sent real data for Kathmandu" every 30s.
- Verify: `kafka-console-consumer --topic nepal-weather --bootstrap-server localhost:9092 --from-beginning` (install Kafka tools).

**Notes**:
- Falls back to simulation if API fails.
- Customize interval: Edit `start_feeding_data(interval_seconds=60)`.
- Stations: 8 hardcoded (Kathmandu, Pokhara, etc.); extend in `nepali_stations`.

### 2. Run the Main System (Consumer + Dashboard)
This consumes from Kafka (or simulates), processes, and shows the dashboard. Run in another terminal.

```bash
python nepal_weather_system.py
```

- Output: Banner, thread starts, live dashboard refreshes (clear screen every 5s).
- Dashboard shows: Weather table, active alerts (e.g., "🚨 ACTIVE ALERTS"), stats (avg temp, high-risk count), tool status.
- Press **Ctrl+C** to stop gracefully (logs batches/alerts).

**Modes**:
- **With Feeder**: Real data flows; dashboard reflects live Nepal weather (e.g., October 2025: ~20–28°C, low rain).
- **Standalone**: Uses internal simulator (6 districts); generates extremes 25% of time.
- Customize: Edit `Config` (thresholds, intervals, districts).

### Example Dashboard Snippet
```
🌤️  NEPAL REAL-TIME WEATHER MONITORING & EARLY WARNING SYSTEM
...
📊 CURRENT WEATHER CONDITIONS
------------------------------------------------------------------------------------------------------------------------------------------------
     District  Temp (°C)  Rain (mm)  Rain 1h (mm)  River (m) Flood Risk Landslide Risk       Alert Level
   Kathmandu       22.5       0.0          0.0       2.5     🟢 LOW        🟢 LOW           ✅ NORMAL
     Pokhara       24.1       1.2          0.5       3.1     🟡 MODERATE   🟢 LOW           🔶 MODERATE
...
🚨 ACTIVE ALERTS (1 Districts)
...
📈 SYSTEM STATISTICS
...
🛠️  BIG DATA TOOLS STATUS
...
```

## Configuration

- **Risk Thresholds** (`Config.RISK_THRESHOLDS`): Tune for floods/landslides (e.g., extreme flood: 75mm/h precip).
- **Districts** (`Config.DISTRICTS`): Add more (name, region, lat/lon, elevation).
- **Intervals**: `DATA_GENERATION_INTERVAL=10s`, `BATCH_PROCESSING_INTERVAL=30s`, etc.
- **API Fallbacks**: In feeder, simulated data uses Nepal-specific ranges (e.g., Kathmandu: 15–28°C).

## Development & Customization

- **Sync Districts**: Feeder has 8 stations; system monitors 6—align in `Config.DISTRICTS`.
- **Add Real Hydrology**: Integrate Nepal DHM APIs (e.g., for river/soil) in `fetch_real_station_data`.
- **Web Dashboard**: Replace console with Flask/Dash; use `dashboard.weather_data` for API.
- **ML Enhancements**: Use Dask-ML for predictive risks (e.g., LSTM on precip trends).
- **Testing**: Run without Kafka/Redis (fallbacks auto-engage). Unit tests: Add via `pytest`.
- **Logging**: Prints to console; integrate `logging` for files.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `ValueError: WEATHER_API_KEY not set` | `export WEATHER_API_KEY=your_key` |
| Kafka connection fails | Start Kafka; check `localhost:9092`. Fallback: Simulator runs. |
| Dask/Redis unavailable | Install pkgs; system warns and uses Pandas/in-memory. |
| No live data | Check API key/internet; fallback activates. |
| Dashboard not updating | Threads staggered—wait 10s; check `running=True`. |
| High CPU | Increase sleep intervals in threads. |
| **Bold text not rendering** in README.md | Ensure no leading spaces before `**text**` (e.g., "**Bold**" not " **Bold**"). Check raw file view on GitHub (click "Raw" button). Save as UTF-8 without BOM. If in lists/tables, verify indentation (use 2 spaces for sub-items). Test with simple `**test**` in a new .md file. |

- Logs: Watch for "❌" errors (e.g., "API error").
- Debug: Add `print(processed_data)` in threads.

## Contributing

1. Fork the repo.
2. Create a feature branch (`git checkout -b feature/amazing-feature`).
3. Commit changes (`git commit -m 'Add amazing feature'`).
4. Push (`git push origin feature/amazing-feature`).
5. Open a Pull Request.

Focus: More APIs (e.g., OpenWeatherMap), cloud deployment (AWS/GCP), mobile alerts.

## License

MIT License – Feel free to use/modify. © 2025 [Your Name/Org].

## Acknowledgments

- [WeatherAPI.com](https://www.weatherapi.com/) for free weather data.
- Nepal Department of Hydrology & Meteorology (DHM) for inspiration.
- Open-source: Kafka, Dask, Redis, Pandas.

---

*Last Updated: October 06, 2025*  
Questions? Open an issue or email [your-contact]. 🚀

### Quick Fix for Bold Rendering
I've double-checked the Markdown syntax—**bold** should render fine on GitHub as long as:
- There's no space immediately after the opening `**` or before the closing `**` (e.g., `**bold**` not `* *bold* *`).
- The file is named `README.md` (case-sensitive) and committed to the repo root.
- No HTML conflicts or escaped characters.

To test: Create a minimal `test.md` with just `**Hello bold world**` and view it on GitHub. If it fails, it might be a repo/cache issue—try editing/committing again or clearing browser cache.

Copy the full content above directly into your `README.md` file (use a plain text editor like VS Code to avoid hidden characters). If issues persist, share a screenshot of the raw file!
