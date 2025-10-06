#!/usr/bin/env python3
"""
Nepal Real-time Weather Monitoring & Early Warning System
Big Data Pipeline with Kafka, Dask, Redis and Live Dashboard
"""

import time
import random
import json
import threading
from datetime import datetime, timedelta
from collections import deque, defaultdict
import pandas as pd
import numpy as np
import os
import sys
from typing import Dict, List, Any

# Try to import big data tools (fallback to simulated versions if not available)
try:
    from kafka import KafkaProducer, KafkaConsumer
    KAFKA_AVAILABLE = True
except ImportError:
    print("⚠️  kafka-python not available, using simulated Kafka")
    KAFKA_AVAILABLE = False

try:
    import dask.dataframe as dd
    from dask.distributed import Client, LocalCluster
    DASK_AVAILABLE = True
except ImportError:
    print("⚠️  Dask not available, using pandas for processing")
    DASK_AVAILABLE = False

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    print("⚠️  Redis not available, using in-memory storage")
    REDIS_AVAILABLE = False

# =============================================================================
# CONFIGURATION
# =============================================================================

class Config:
    """System configuration constants"""
    # Nepal districts with geographical data
    DISTRICTS = [
        {"name": "Kathmandu", "region": "valley", "lat": 27.7172, "lon": 85.3240, "elevation": 1400},
        {"name": "Pokhara", "region": "hills", "lat": 28.2096, "lon": 83.9856, "elevation": 800},
        {"name": "Biratnagar", "region": "plains", "lat": 26.4525, "lon": 87.2718, "elevation": 200},
        {"name": "Nepalgunj", "region": "plains", "lat": 28.0915, "lon": 81.6720, "elevation": 150},
        {"name": "Dharan", "region": "hills", "lat": 26.8127, "lon": 87.2842, "elevation": 350},
        {"name": "Butwal", "region": "hills", "lat": 27.6833, "lon": 83.4333, "elevation": 450}
    ]
    
    # Risk thresholds (in mm for precipitation, meters for river levels)
    RISK_THRESHOLDS = {
        "flood": {
            "extreme": {"precip_1h": 75, "river_level": 5.0},
            "high": {"precip_1h": 50, "river_level": 4.0},
            "moderate": {"precip_1h": 30, "river_level": 3.0}
        },
        "landslide": {
            "extreme": {"precip": 40, "soil_moisture": 85},
            "high": {"precip": 30, "soil_moisture": 80},
            "moderate": {"precip": 20, "soil_moisture": 70}
        }
    }
    
    # Kafka configuration
    KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
    KAFKA_TOPIC = 'nepal-weather'
    
    # Redis configuration
    REDIS_HOST = 'localhost'
    REDIS_PORT = 6379
    
    # Processing intervals (seconds)
    DATA_GENERATION_INTERVAL = 10
    DASHBOARD_UPDATE_INTERVAL = 5
    BATCH_PROCESSING_INTERVAL = 30

# =============================================================================
# DATA PROCESSING MODULES
# =============================================================================

class WeatherDataGenerator:
    """Generates realistic weather data for Nepal districts"""
    
    def __init__(self, config: Config):
        self.config = config
        self.districts = config.DISTRICTS
        
    def generate_weather_batch(self) -> List[Dict[str, Any]]:
        """
        Generate a batch of realistic weather data for all districts
        Returns: List of weather data dictionaries
        """
        data_batch = []
        current_time = datetime.now()
        
        for district in self.districts:
            # Simulate seasonal variations
            current_month = current_time.month
            is_monsoon = 6 <= current_month <= 9  # June to September
            
            # Base weather patterns based on region and season
            if district["region"] == "plains":
                base_temp = random.uniform(25, 38)
                base_precip = random.uniform(0, 100) if is_monsoon else random.uniform(0, 60)
            elif district["region"] == "hills":
                base_temp = random.uniform(18, 30)
                base_precip = random.uniform(0, 80) if is_monsoon else random.uniform(0, 50)
            else:  # valley
                base_temp = random.uniform(15, 28)
                base_precip = random.uniform(0, 60) if is_monsoon else random.uniform(0, 40)
            
            # Occasionally generate extreme weather events (25% chance)
            is_extreme = random.random() < 0.25
            if is_extreme:
                base_precip *= random.uniform(1.5, 3.0)  # 50-200% increase in precipitation
            
            weather_data = {
                "station_id": f"STN_{district['name'][:3].upper()}",
                "district": district["name"],
                "region": district["region"],
                "timestamp": current_time.isoformat(),
                "temperature": round(base_temp, 1),
                "humidity": round(random.uniform(50, 95), 1),
                "pressure": round(random.uniform(1000, 1015), 1),
                "wind_speed": round(random.uniform(5, 25), 1),
                "wind_direction": random.choice(["N", "S", "E", "W", "NE", "NW", "SE", "SW"]),
                "precipitation": round(base_precip, 1),
                "precipitation_1h": round(random.uniform(0, base_precip), 1),
                "precipitation_24h": round(random.uniform(base_precip, 200), 1),
                "river_level": round(random.uniform(1.5, 6.0), 2),
                "soil_moisture": round(random.uniform(50, 95), 1),
                "latitude": district["lat"],
                "longitude": district["lon"],
                "elevation": district["elevation"],
                "is_extreme": is_extreme,
                "data_quality": "simulated"
            }
            data_batch.append(weather_data)
        
        return data_batch

class RiskCalculator:
    """Calculates flood and landslide risks based on weather data"""
    
    def __init__(self, config: Config):
        self.config = config
        
    def calculate_risks(self, weather_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate flood and landslide risks for given weather data
        Returns: Dictionary with risk assessments
        """
        precip_1h = weather_data['precipitation_1h']
        river_level = weather_data['river_level']
        precip = weather_data['precipitation']
        soil_moisture = weather_data['soil_moisture']
        region = weather_data['region']
        
        # Flood Risk Calculation
        if precip_1h > self.config.RISK_THRESHOLDS["flood"]["extreme"]["precip_1h"] or \
           river_level > self.config.RISK_THRESHOLDS["flood"]["extreme"]["river_level"]:
            flood_risk = "🔴 EXTREME"
            flood_score = 4
        elif precip_1h > self.config.RISK_THRESHOLDS["flood"]["high"]["precip_1h"] or \
             river_level > self.config.RISK_THRESHOLDS["flood"]["high"]["river_level"]:
            flood_risk = "🟠 HIGH"
            flood_score = 3
        elif precip_1h > self.config.RISK_THRESHOLDS["flood"]["moderate"]["precip_1h"]:
            flood_risk = "🟡 MODERATE"
            flood_score = 2
        else:
            flood_risk = "🟢 LOW"
            flood_score = 1
        
        # Landslide Risk Calculation (primarily for hilly regions)
        if region == "hills":
            if precip > self.config.RISK_THRESHOLDS["landslide"]["extreme"]["precip"] and \
               soil_moisture > self.config.RISK_THRESHOLDS["landslide"]["extreme"]["soil_moisture"]:
                landslide_risk = "🔴 EXTREME"
                landslide_score = 4
            elif precip > self.config.RISK_THRESHOLDS["landslide"]["high"]["precip"] and \
                 soil_moisture > self.config.RISK_THRESHOLDS["landslide"]["high"]["soil_moisture"]:
                landslide_risk = "🟠 HIGH"
                landslide_score = 3
            elif precip > self.config.RISK_THRESHOLDS["landslide"]["moderate"]["precip"]:
                landslide_risk = "🟡 MODERATE"
                landslide_score = 2
            else:
                landslide_risk = "🟢 LOW"
                landslide_score = 1
        else:
            landslide_risk = "🟢 LOW"
            landslide_score = 1
        
        # Overall Alert Level
        max_score = max(flood_score, landslide_score)
        if max_score == 4:
            overall_alert = "🚨 CRITICAL"
        elif max_score == 3:
            overall_alert = "⚠️ HIGH ALERT"
        elif max_score == 2:
            overall_alert = "🔶 MODERATE"
        else:
            overall_alert = "✅ NORMAL"
        
        return {
            'flood_risk': flood_risk,
            'landslide_risk': landslide_risk,
            'overall_alert': overall_alert,
            'risk_score': max_score,
            'processed_time': datetime.now().isoformat()
        }

# =============================================================================
# BIG DATA PROCESSING MODULES
# =============================================================================

class KafkaManager:
    """Manages Kafka producer and consumer for real-time data streaming"""
    
    def __init__(self, config: Config):
        self.config = config
        self.producer = None
        self.consumer = None
        self.setup_kafka()
        
    def setup_kafka(self):
        """Initialize Kafka connections"""
        if not KAFKA_AVAILABLE:
            print("⚠️  Kafka not available, using simulated messaging")
            return
            
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                batch_size=16384,
                linger_ms=10
            )
            
            self.consumer = KafkaConsumer(
                self.config.KAFKA_TOPIC,
                bootstrap_servers=self.config.KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='latest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='nepal-weather-group',
                consumer_timeout_ms=5000
            )
            
            print("✅ Kafka manager initialized successfully")
        except Exception as e:
            print(f"❌ Kafka setup failed: {e}")
    
    def produce_message(self, data: Dict[str, Any]):
        """Send message to Kafka topic"""
        if self.producer:
            try:
                self.producer.send(self.config.KAFKA_TOPIC, data)
                self.producer.flush()
            except Exception as e:
                print(f"❌ Kafka produce error: {e}")
    
    def consume_messages(self) -> List[Dict[str, Any]]:
        """Consume messages from Kafka topic"""
        if not self.consumer:
            return []
            
        messages = []
        try:
            kafka_messages = self.consumer.poll(timeout_ms=1000)
            for topic_partition, message_list in kafka_messages.items():
                for message in message_list:
                    messages.append(message.value)
        except Exception as e:
            print(f"❌ Kafka consume error: {e}")
            
        return messages

class DaskProcessor:
    """Handles distributed data processing using Dask"""
    
    def __init__(self, config: Config):
        self.config = config
        self.client = None
        self.setup_dask()
        
    def setup_dask(self):
        """Initialize Dask distributed cluster"""
        if not DASK_AVAILABLE:
            print("⚠️  Dask not available, using pandas for processing")
            return
            
        try:
            self.client = Client(n_workers=2, threads_per_worker=2, memory_limit='1GB')
            print(f"✅ Dask cluster started: {self.client.dashboard_link}")
        except Exception as e:
            print(f"❌ Dask setup failed: {e}")
    
    def process_batch_data(self, data_batch: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Process a batch of weather data using Dask for parallel computing
        Returns: Processed DataFrame with risk calculations
        """
        if not DASK_AVAILABLE or not self.client:
            # Fallback to pandas processing
            return self._process_with_pandas(data_batch)
        
        try:
            # Convert to Dask DataFrame
            df = dd.from_pandas(pd.DataFrame(data_batch), npartitions=2)
            
            # Define risk calculation function for Dask
            def calculate_risk_row(row):
                precip_1h = row['precipitation_1h']
                river_level = row['river_level']
                precip = row['precipitation']
                soil_moisture = row['soil_moisture']
                region = row['region']
                
                # Flood risk
                if precip_1h > 75 or river_level > 5.0:
                    flood_risk = "🔴 EXTREME"
                elif precip_1h > 50 or river_level > 4.0:
                    flood_risk = "🟠 HIGH"
                elif precip_1h > 30:
                    flood_risk = "🟡 MODERATE"
                else:
                    flood_risk = "🟢 LOW"
                
                # Landslide risk
                if region == "hills" and precip > 40 and soil_moisture > 85:
                    landslide_risk = "🔴 EXTREME"
                elif region == "hills" and precip > 30 and soil_moisture > 80:
                    landslide_risk = "🟠 HIGH"
                elif region == "hills" and precip > 20:
                    landslide_risk = "🟡 MODERATE"
                else:
                    landslide_risk = "🟢 LOW"
                
                return pd.Series([flood_risk, landslide_risk])
            
            # Apply risk calculation
            risk_cols = df.apply(calculate_risk_row, axis=1, meta=('flood_risk', 'object'), 
                               result_type='expand')
            df = df.assign(flood_risk=risk_cols[0], landslide_risk=risk_cols[1])
            
            # Compute results
            result_df = df.compute()
            print("✅ Dask batch processing completed")
            return result_df
            
        except Exception as e:
            print(f"❌ Dask processing error: {e}")
            return self._process_with_pandas(data_batch)
    
    def _process_with_pandas(self, data_batch: List[Dict[str, Any]]) -> pd.DataFrame:
        """Fallback processing with pandas"""
        df = pd.DataFrame(data_batch)
        risk_calc = RiskCalculator(self.config)
        
        risks = []
        for _, row in df.iterrows():
            risk_data = risk_calc.calculate_risks(row.to_dict())
            risks.append(risk_data)
        
        risk_df = pd.DataFrame(risks)
        result_df = pd.concat([df, risk_df], axis=1)
        print("✅ Pandas batch processing completed")
        return result_df

class RedisManager:
    """Manages Redis for real-time data storage and caching"""
    
    def __init__(self, config: Config):
        self.config = config
        self.redis_client = None
        self.setup_redis()
        
    def setup_redis(self):
        """Initialize Redis connection"""
        if not REDIS_AVAILABLE:
            print("⚠️  Redis not available, using in-memory storage")
            return
            
        try:
            self.redis_client = redis.Redis(
                host=self.config.REDIS_HOST,
                port=self.config.REDIS_PORT,
                db=0,
                decode_responses=True
            )
            # Test connection
            self.redis_client.ping()
            print("✅ Redis connected successfully")
        except Exception as e:
            print(f"❌ Redis connection failed: {e}")
    
    def store_alert(self, alert_data: Dict[str, Any]):
        """Store alert in Redis"""
        if not self.redis_client:
            return
            
        try:
            alert_id = f"alert:{datetime.now().strftime('%Y%m%d%H%M%S')}"
            self.redis_client.hset(alert_id, mapping=alert_data)
            self.redis_client.expire(alert_id, 86400)  # Expire in 24 hours
            self.redis_client.lpush('recent_alerts', alert_id)
            self.redis_client.ltrim('recent_alerts', 0, 49)  # Keep last 50 alerts
        except Exception as e:
            print(f"❌ Redis store error: {e}")
    
    def get_recent_alerts(self, count: int = 10) -> List[Dict[str, Any]]:
        """Get recent alerts from Redis"""
        if not self.redis_client:
            return []
            
        try:
            alert_ids = self.redis_client.lrange('recent_alerts', 0, count-1)
            alerts = []
            for alert_id in alert_ids:
                alert_data = self.redis_client.hgetall(alert_id)
                if alert_data:
                    alerts.append(alert_data)
            return alerts
        except Exception as e:
            print(f"❌ Redis get error: {e}")
            return []

# =============================================================================
# DASHBOARD AND VISUALIZATION
# =============================================================================

class WeatherDashboard:
    """Real-time dashboard for weather monitoring"""
    
    def __init__(self, config: Config):
        self.config = config
        self.weather_data = {}
        self.alerts_history = deque(maxlen=100)
        self.metrics_history = deque(maxlen=50)
        
    def clear_console(self):
        """Clear console for dashboard display"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def display_dashboard(self, processed_data: Dict[str, Any] = None):
        """
        Display the main weather monitoring dashboard
        Args:
            processed_data: Latest processed weather data
        """
        self.clear_console()
        
        # Header
        print("🌤️  NEPAL REAL-TIME WEATHER MONITORING & EARLY WARNING SYSTEM")
        print("=" * 100)
        print(f"📍 Monitoring {len(self.config.DISTRICTS)} Districts | ⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 100)
        
        if not self.weather_data:
            print("\n📊 Waiting for weather data...")
            return
        
        # Current Weather Table
        self._display_weather_table()
        
        # Alerts Section
        self._display_alerts_section()
        
        # Statistics Section
        self._display_statistics()
        
        # System Status
        self._display_system_status()
    
    def _display_weather_table(self):
        """Display current weather conditions table"""
        print("\n📊 CURRENT WEATHER CONDITIONS")
        print("-" * 100)
        
        table_data = []
        for district, data in self.weather_data.items():
            table_data.append({
                'District': district,
                'Temp (°C)': data.get('temperature', 'N/A'),
                'Rain (mm)': data.get('precipitation', 'N/A'),
                'Rain 1h (mm)': data.get('precipitation_1h', 'N/A'),
                'River (m)': data.get('river_level', 'N/A'),
                'Flood Risk': data.get('flood_risk', '🟢 LOW'),
                'Landslide Risk': data.get('landslide_risk', '🟢 LOW'),
                'Alert Level': data.get('overall_alert', '✅ NORMAL')
            })
        
        df = pd.DataFrame(table_data)
        print(df.to_string(index=False, justify='center'))
    
    def _display_alerts_section(self):
        """Display active alerts section"""
        high_risk_districts = []
        for district, data in self.weather_data.items():
            if "EXTREME" in data.get('flood_risk', '') or "HIGH" in data.get('flood_risk', '') or \
               "EXTREME" in data.get('landslide_risk', '') or "HIGH" in data.get('landslide_risk', ''):
                high_risk_districts.append((district, data))
        
        if high_risk_districts:
            print(f"\n🚨 ACTIVE ALERTS ({len(high_risk_districts)} Districts)")
            print("-" * 100)
            for district, data in high_risk_districts:
                alerts = []
                if "EXTREME" in data.get('flood_risk', '') or "HIGH" in data.get('flood_risk', ''):
                    alerts.append(f"FLOOD({data['flood_risk']})")
                if "EXTREME" in data.get('landslide_risk', '') or "HIGH" in data.get('landslide_risk', ''):
                    alerts.append(f"LANDSLIDE({data['landslide_risk']})")
                
                print(f"📍 {district:12} | {' | '.join(alerts):30} | 💧 {data['precipitation_1h']:5.1f}mm/h | 🌊 {data['river_level']:4.1f}m")
        else:
            print(f"\n✅ ALL DISTRICTS AT NORMAL RISK LEVELS")
    
    def _display_statistics(self):
        """Display system statistics"""
        if not self.weather_data:
            return
            
        # Calculate statistics
        temperatures = [data['temperature'] for data in self.weather_data.values()]
        precipitation = [data['precipitation'] for data in self.weather_data.values()]
        precipitation_1h = [data['precipitation_1h'] for data in self.weather_data.values()]
        
        high_risk_count = sum(1 for data in self.weather_data.values() 
                            if "EXTREME" in data.get('flood_risk', '') or "HIGH" in data.get('flood_risk', '') or
                            "EXTREME" in data.get('landslide_risk', '') or "HIGH" in data.get('landslide_risk', ''))
        
        print(f"\n📈 SYSTEM STATISTICS")
        print("-" * 100)
        print(f"🌡️  Avg Temperature: {np.mean(temperatures):.1f}°C | 💧 Avg Rainfall: {np.mean(precipitation):.1f}mm")
        print(f"📊 High Risk Districts: {high_risk_count}/{len(self.weather_data)} | 📈 Max Rain (1h): {max(precipitation_1h):.1f}mm")
        print(f"🚨 Total Alerts: {len(self.alerts_history)} | ⏰ Uptime: {self._get_uptime()}")
    
    def _display_system_status(self):
        """Display big data tools status"""
        print(f"\n🛠️  BIG DATA TOOLS STATUS")
        print("-" * 100)
        status = [
            f"Kafka: {'✅' if KAFKA_AVAILABLE else '❌'}",
            f"Dask: {'✅' if DASK_AVAILABLE else '❌'}",
            f"Redis: {'✅' if REDIS_AVAILABLE else '❌'}",
            f"Pandas: ✅",
            f"Streaming: ✅"
        ]
        print(" | ".join(status))
        print("=" * 100)
    
    def _get_uptime(self) -> str:
        """Calculate and format system uptime"""
        if not hasattr(self, 'start_time'):
            self.start_time = datetime.now()
        
        uptime = datetime.now() - self.start_time
        hours = uptime.seconds // 3600
        minutes = (uptime.seconds % 3600) // 60
        return f"{hours:02d}:{minutes:02d}"
    
    def update_data(self, district: str, weather_data: Dict[str, Any]):
        """Update weather data for a district"""
        self.weather_data[district] = weather_data
        
        # Check if this is a new alert
        if "EXTREME" in weather_data.get('flood_risk', '') or "HIGH" in weather_data.get('flood_risk', '') or \
           "EXTREME" in weather_data.get('landslide_risk', '') or "HIGH" in weather_data.get('landslide_risk', ''):
            
            alert = {
                'timestamp': datetime.now().isoformat(),
                'district': district,
                'flood_risk': weather_data.get('flood_risk', ''),
                'landslide_risk': weather_data.get('landslide_risk', ''),
                'precipitation_1h': weather_data.get('precipitation_1h', 0),
                'river_level': weather_data.get('river_level', 0),
                'temperature': weather_data.get('temperature', 0)
            }
            self.alerts_history.append(alert)

# =============================================================================
# MAIN SYSTEM INTEGRATION
# =============================================================================

class NepalWeatherSystem:
    """Main system integrating all components"""
    
    def __init__(self):
        self.config = Config()
        self.running = False
        
        # Initialize components
        self.data_generator = WeatherDataGenerator(self.config)
        self.risk_calculator = RiskCalculator(self.config)
        self.kafka_manager = KafkaManager(self.config)
        self.dask_processor = DaskProcessor(self.config)
        self.redis_manager = RedisManager(self.config)
        self.dashboard = WeatherDashboard(self.config)
        
        # Data storage
        self.data_buffer = deque(maxlen=1000)
        self.batch_count = 0
        
        print("🚀 Nepal Weather Monitoring System Initialized")
    
    def data_generation_thread(self):
        """Thread for generating and streaming weather data"""
        print("🔄 Starting Data Generation Thread...")
        
        while self.running:
            try:
                # Generate new weather data
                data_batch = self.data_generator.generate_weather_batch()
                
                # Process each district's data
                for weather_data in data_batch:
                    # Calculate risks
                    risk_data = self.risk_calculator.calculate_risks(weather_data)
                    processed_data = {**weather_data, **risk_data}
                    
                    # Update dashboard
                    self.dashboard.update_data(weather_data['district'], processed_data)
                    
                    # Send to Kafka (if available)
                    self.kafka_manager.produce_message(processed_data)
                    
                    # Store in buffer for batch processing
                    self.data_buffer.append(processed_data)
                
                # Log generation
                extreme_count = sum(1 for data in data_batch if data['is_extreme'])
                print(f"📤 Generated {len(data_batch)} records ({extreme_count} extreme events)")
                
                time.sleep(self.config.DATA_GENERATION_INTERVAL)
                
            except Exception as e:
                print(f"❌ Data generation error: {e}")
                time.sleep(self.config.DATA_GENERATION_INTERVAL)
    
    def data_processing_thread(self):
        """Thread for processing consumed data"""
        print("🔄 Starting Data Processing Thread...")
        
        while self.running:
            try:
                # Consume from Kafka
                if KAFKA_AVAILABLE:
                    messages = self.kafka_manager.consume_messages()
                    for message in messages:
                        # Update dashboard with consumed data
                        self.dashboard.update_data(message['district'], message)
                
                time.sleep(1)  # Small delay to prevent CPU overload
                
            except Exception as e:
                print(f"❌ Data processing error: {e}")
                time.sleep(1)
    
    def batch_processing_thread(self):
        """Thread for batch processing with Dask"""
        print("🔄 Starting Batch Processing Thread...")
        
        while self.running:
            try:
                if len(self.data_buffer) >= 50:  # Process when we have enough data
                    self.batch_count += 1
                    print(f"🔍 Starting batch processing #{self.batch_count}...")
                    
                    # Convert buffer to list for processing
                    batch_data = list(self.data_buffer)
                    
                    # Process with Dask
                    processed_batch = self.dask_processor.process_batch_data(batch_data)
                    
                    # Analyze results
                    high_risk_count = len(processed_batch[
                        (processed_batch['flood_risk'].str.contains('EXTREME|HIGH')) |
                        (processed_batch['landslide_risk'].str.contains('EXTREME|HIGH'))
                    ])
                    
                    print(f"✅ Batch #{self.batch_count} processed: {len(processed_batch)} records, {high_risk_count} high risk")
                    
                    # Clear buffer after processing
                    self.data_buffer.clear()
                
                time.sleep(self.config.BATCH_PROCESSING_INTERVAL)
                
            except Exception as e:
                print(f"❌ Batch processing error: {e}")
                time.sleep(self.config.BATCH_PROCESSING_INTERVAL)
    
    def dashboard_thread(self):
        """Thread for updating the dashboard"""
        print("🔄 Starting Dashboard Thread...")
        
        while self.running:
            try:
                self.dashboard.display_dashboard()
                time.sleep(self.config.DASHBOARD_UPDATE_INTERVAL)
            except Exception as e:
                print(f"❌ Dashboard error: {e}")
                time.sleep(self.config.DASHBOARD_UPDATE_INTERVAL)
    
    def start_system(self):
        """Start the complete weather monitoring system"""
        print("\n" + "="*80)
        print("🚀 NEPAL REAL-TIME WEATHER MONITORING & EARLY WARNING SYSTEM")
        print("="*80)
        print("📊 Features:")
        print("  • Real-time weather data generation")
        print("  • Flood and landslide risk assessment")
        print("  • Apache Kafka streaming pipeline")
        print("  • Dask distributed processing")
        print("  • Redis alert storage")
        print("  • Live dashboard visualization")
        print(f"📍 Monitoring {len(self.config.DISTRICTS)} districts across Nepal")
        print("="*80)
        
        self.running = True
        
        # Start all threads
        threads = []
        
        thread_functions = [
            self.data_generation_thread,
            self.data_processing_thread, 
            self.batch_processing_thread,
            self.dashboard_thread
        ]
        
        for thread_func in thread_functions:
            thread = threading.Thread(target=thread_func, daemon=True)
            thread.start()
            threads.append(thread)
            time.sleep(1)  # Stagger thread starts
        
        print("✅ All system threads started successfully")
        print("💡 Press Ctrl+C to stop the system")
        
        try:
            # Keep main thread alive
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 Received shutdown signal...")
        finally:
            self.stop_system()
    
    def stop_system(self):
        """Stop the system gracefully"""
        print("\n🛑 Stopping Nepal Weather System...")
        self.running = False
        
        # Close connections
        if hasattr(self.dask_processor, 'client') and self.dask_processor.client:
            self.dask_processor.client.close()
        
        print("✅ System shutdown complete")
        print(f"📊 Final Stats: {self.batch_count} batches processed, {len(self.dashboard.alerts_history)} alerts generated")

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    """Main entry point for the application"""
    
    print("🔧 Nepal Weather System - Starting...")
    
    # Check for required packages
    missing_packages = []
    if not KAFKA_AVAILABLE:
        missing_packages.append("kafka-python")
    if not DASK_AVAILABLE:
        missing_packages.append("dask[complete]")
    if not REDIS_AVAILABLE:
        missing_packages.append("redis")
    
    if missing_packages:
        print(f"⚠️  Missing optional packages: {', '.join(missing_packages)}")
        print("💡 Install with: pip install " + " ".join(missing_packages))
        print("   The system will run with reduced functionality\n")
    
    # Create and start the system
    weather_system = NepalWeatherSystem()
    
    try:
        weather_system.start_system()
    except Exception as e:
        print(f"❌ System error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
