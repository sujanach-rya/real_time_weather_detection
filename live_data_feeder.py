# live_data_feeder.py
import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime

class NepalDataFeeder:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        # Nepal-specific data sources
        self.data_sources = {
            "dhm": "https://api.dhm.gov.np/weather",  # Dept of Hydrology & Meteorology
            "openweather": "http://api.openweathermap.org/data/2.5/weather",
            "rainfall": "https://dhm.gov.np/api/rainfall"
        }
    
    def fetch_live_weather_data(self):
        """Fetch real weather data from Nepal government sources"""
        
        # Nepal's major weather stations
        nepali_stations = [
            {"id": "KTM", "name": "Kathmandu", "lat": 27.7172, "lon": 85.3240},
            {"id": "PKR", "name": "Pokhara", "lat": 28.2096, "lon": 83.9856},
            {"id": "BRT", "name": "Biratnagar", "lat": 26.4525, "lon": 87.2718},
            {"id": "NPL", "name": "Nepalgunj", "lat": 28.0915, "lon": 81.6720},
            {"id": "BDR", "name": "Bhadrapur", "lat": 26.5700, "lon": 88.0796},
            {"id": "DHI", "name": "Dharan", "lat": 26.8127, "lon": 87.2842},
            {"id": "BKT", "name": "Bhaktapur", "lat": 27.6710, "lon": 85.4298},
            {"id": "LTP", "name": "Letang", "lat": 26.7500, "lon": 87.4167}
        ]
        
        weather_data = []
        for station in nepali_stations:
            try:
                # For demo - simulate real data. Replace with actual API calls
                weather_record = self.simulate_station_data(station)
                weather_data.append(weather_record)
                
                # Send to Kafka
                self.producer.send('nepal-weather', value=weather_record)
                print(f"📡 Sent data for {station['name']}")
                
            except Exception as e:
                print(f"❌ Error fetching data for {station['name']}: {e}")
        
        return weather_data
    
    def simulate_station_data(self, station):
        """Simulate realistic weather data for Nepal stations"""
        import random
        import numpy as np
        
        # Nepal-specific weather patterns
        base_temps = {
            "Kathmandu": random.uniform(15, 28),
            "Pokhara": random.uniform(18, 30),
            "Biratnagar": random.uniform(22, 35),
            "Nepalgunj": random.uniform(25, 38),
            "Bhadrapur": random.uniform(20, 32),
            "Dharan": random.uniform(18, 30),
            "Bhaktapur": random.uniform(16, 27),
            "Letang": random.uniform(17, 29)
        }
        
        # Monsoon season adjustments (June-September)
        current_month = datetime.now().month
        is_monsoon = 6 <= current_month <= 9
        
        return {
            "station_id": station["id"],
            "timestamp": datetime.now().isoformat(),
            "temperature": round(base_temps.get(station["name"], 25), 2),
            "humidity": random.uniform(60, 95) if is_monsoon else random.uniform(40, 80),
            "pressure": round(random.uniform(900, 1015), 2),
            "wind_speed": round(random.uniform(5, 25), 2),
            "wind_direction": random.choice(["N", "S", "E", "W", "NE", "NW", "SE", "SW"]),
            "precipitation": round(random.uniform(0, 50) if is_monsoon else random.uniform(0, 20), 2),
            "precipitation_1h": round(random.uniform(0, 30), 2),
            "precipitation_24h": round(random.uniform(0, 150), 2),
            "river_level": round(random.uniform(1.5, 6.0), 2),
            "soil_moisture": round(random.uniform(50, 95), 2),
            "latitude": station["lat"],
            "longitude": station["lon"],
            "elevation": round(random.uniform(100, 1400), 2),
            "district": station["name"]
        }
    
    def start_feeding_data(self, interval_seconds=30):
        """Continuously feed data to the system"""
        print("🚀 Starting Nepal Weather Data Feeder...")
        try:
            while True:
                self.fetch_live_weather_data()
                print(f"⏰ Waiting {interval_seconds} seconds for next update...")
                time.sleep(interval_seconds)
        except KeyboardInterrupt:
            print("🛑 Data feeder stopped")

# Run the data feeder
if __name__ == "__main__":
    feeder = NepalDataFeeder()
    feeder.start_feeding_data(interval_seconds=30)  # Update every 30 seconds
