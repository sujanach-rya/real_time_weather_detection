# fixed_monitor.py
from kafka import KafkaConsumer, KafkaProducer
import json
import time
import random
from datetime import datetime
import pandas as pd
import threading

class NepalWeatherMonitor:
    def __init__(self):
        self.setup_components()
        self.weather_data = {}
        self.alerts_history = []
        
    def setup_components(self):
        """Setup all system components"""
        self.producer = self.setup_producer()
        self.consumer = self.setup_consumer()
        
    def setup_producer(self):
        """Setup Kafka producer for data generation"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=['localhost:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ Data producer connected")
            return producer
        except Exception as e:
            print(f"❌ Producer setup failed: {e}")
            return None
    
    def setup_consumer(self):
        """Setup Kafka consumer for data processing"""
        try:
            consumer = KafkaConsumer(
                'nepal-weather',
                bootstrap_servers=['localhost:9092'],
                auto_offset_reset='latest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=5000
            )
            print("✅ Data consumer connected")
            return consumer
        except Exception as e:
            print(f"❌ Consumer setup failed: {e}")
            return None
    
    def generate_weather_data(self):
        """Generate realistic Nepal weather data"""
        districts = [
            {"name": "Kathmandu", "risk": "valley"},
            {"name": "Pokhara", "risk": "hills"}, 
            {"name": "Biratnagar", "risk": "plains"},
            {"name": "Nepalgunj", "risk": "plains"},
            {"name": "Dharan", "risk": "hills"},
            {"name": "Butwal", "risk": "hills"}
        ]
        
        data_batch = []
        for district in districts:
            # Occasionally generate extreme weather (30% chance)
            is_extreme = random.random() < 0.3
            
            if district["risk"] == "plains":
                temp = random.uniform(25, 38)
                precip = random.uniform(0, 100) if is_extreme else random.uniform(0, 40)
            elif district["risk"] == "hills":
                temp = random.uniform(18, 30)
                precip = random.uniform(0, 80) if is_extreme else random.uniform(0, 35)
            else:  # valley
                temp = random.uniform(15, 28)
                precip = random.uniform(0, 60) if is_extreme else random.uniform(0, 30)
            
            weather_data = {
                "station_id": f"{district['name'][:3].upper()}",
                "district": district["name"],
                "timestamp": datetime.now().isoformat(),
                "temperature": round(temp, 1),
                "humidity": round(random.uniform(50, 95), 1),
                "pressure": round(random.uniform(1000, 1015), 1),
                "wind_speed": round(random.uniform(5, 25), 1),
                "precipitation": round(precip, 1),
                "precipitation_1h": round(random.uniform(0, precip), 1),
                "river_level": round(random.uniform(1.5, 6.0), 2),
                "soil_moisture": round(random.uniform(50, 95), 1),
                "risk_level": district["risk"],
                "is_extreme": is_extreme
            }
            data_batch.append(weather_data)
        
        return data_batch
    
    def calculate_risks(self, weather_data):
        """Calculate flood and landslide risks - FIXED VERSION"""
        try:
            processed = weather_data.copy()
            
            # Ensure we have the required fields with default values
            precip_1h = processed.get('precipitation_1h', 0)
            river_level = processed.get('river_level', 0)
            precip = processed.get('precipitation', 0)
            soil_moisture = processed.get('soil_moisture', 50)
            risk_level = processed.get('risk_level', 'valley')
            
            # Flood risk calculation
            if precip_1h > 75 or river_level > 5.0:
                processed['flood_risk'] = "🔴 EXTREME"
            elif precip_1h > 50 or river_level > 4.0:
                processed['flood_risk'] = "🟠 HIGH" 
            elif precip_1h > 30:
                processed['flood_risk'] = "🟡 MODERATE"
            else:
                processed['flood_risk'] = "🟢 LOW"
            
            # Landslide risk calculation
            if risk_level == "hills" and precip > 40 and soil_moisture > 85:
                processed['landslide_risk'] = "🔴 EXTREME"
            elif risk_level == "hills" and precip > 30 and soil_moisture > 80:
                processed['landslide_risk'] = "🟠 HIGH"
            elif risk_level == "hills" and precip > 20:
                processed['landslide_risk'] = "🟡 MODERATE"
            else:
                processed['landslide_risk'] = "🟢 LOW"
            
            # Overall alert level
            if "EXTREME" in processed['flood_risk'] or "EXTREME" in processed['landslide_risk']:
                processed['overall_alert'] = "🚨 CRITICAL"
            elif "HIGH" in processed['flood_risk'] or "HIGH" in processed['landslide_risk']:
                processed['overall_alert'] = "⚠️ HIGH"
            else:
                processed['overall_alert'] = "✅ NORMAL"
            
            processed['processed_time'] = datetime.now().isoformat()
            
            return processed
            
        except Exception as e:
            print(f"❌ Error in calculate_risks: {e}")
            print(f"   Weather data: {weather_data}")
            # Return original data with default risks
            weather_data['flood_risk'] = "🟢 LOW"
            weather_data['landslide_risk'] = "🟢 LOW"
            weather_data['overall_alert'] = "✅ NORMAL"
            return weather_data
    
    def analyze_alerts(self, processed_data):
        """Analyze and generate alerts"""
        alerts = []
        
        try:
            flood_risk = processed_data.get('flood_risk', '🟢 LOW')
            landslide_risk = processed_data.get('landslide_risk', '🟢 LOW')
            precip_1h = processed_data.get('precipitation_1h', 0)
            precip = processed_data.get('precipitation', 0)
            
            if "EXTREME" in flood_risk:
                alerts.append({
                    "type": "FLOOD",
                    "severity": "🔴 EXTREME",
                    "message": f"EXTREME FLOOD RISK! {precip_1h}mm/h rainfall",
                    "actions": ["IMMEDIATE EVACUATION", "Move to higher ground"]
                })
            elif "HIGH" in flood_risk:
                alerts.append({
                    "type": "FLOOD", 
                    "severity": "🟠 HIGH",
                    "message": f"High flood risk: {precip_1h}mm/h rainfall",
                    "actions": ["Prepare for evacuation", "Monitor river levels"]
                })
            
            if "EXTREME" in landslide_risk:
                alerts.append({
                    "type": "LANDSLIDE",
                    "severity": "🔴 EXTREME",
                    "message": f"EXTREME LANDSLIDE RISK! {precip}mm rain",
                    "actions": ["EVACUATE HILLSIDES", "Avoid slopes"]
                })
            elif "HIGH" in landslide_risk:
                alerts.append({
                    "type": "LANDSLIDE",
                    "severity": "🟠 HIGH", 
                    "message": f"High landslide risk",
                    "actions": ["Avoid hillside travel", "Watch for soil movement"]
                })
        except Exception as e:
            print(f"❌ Error in analyze_alerts: {e}")
            
        return alerts
    
    def display_dashboard(self):
        """Display real-time dashboard"""
        # Clear console (works on most systems)
        print("\033[H\033[J", end="")
        
        print("🌤️  NEPAL WEATHER MONITORING SYSTEM")
        print("=" * 80)
        print(f"Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        if not self.weather_data:
            print("\n📊 Waiting for weather data...")
            return
        
        # Create display table
        df_data = []
        for district, data in self.weather_data.items():
            df_data.append({
                'District': district,
                'Temp (°C)': data.get('temperature', 'N/A'),
                'Rain (mm)': data.get('precipitation', 'N/A'),
                'River (m)': data.get('river_level', 'N/A'),
                'Flood Risk': data.get('flood_risk', '🟢 LOW'),
                'Landslide Risk': data.get('landslide_risk', '🟢 LOW'),
                'Alert Level': data.get('overall_alert', '✅ NORMAL')
            })
        
        df = pd.DataFrame(df_data)
        print("\n" + df.to_string(index=False))
        
        # Show alerts summary
        high_risk = []
        for district, data in self.weather_data.items():
            flood_risk = data.get('flood_risk', '')
            landslide_risk = data.get('landslide_risk', '')
            if "EXTREME" in flood_risk or "HIGH" in flood_risk or "EXTREME" in landslide_risk or "HIGH" in landslide_risk:
                high_risk.append(data)
        
        if high_risk:
            print(f"\n🚨 ACTIVE ALERTS: {len(high_risk)} districts at risk")
            for data in high_risk:
                district = data.get('district', 'Unknown')
                flood = data.get('flood_risk', 'N/A')
                landslide = data.get('landslide_risk', 'N/A')
                print(f"   📍 {district}: Flood={flood}, Landslide={landslide}")
        else:
            print(f"\n✅ All districts at normal risk levels")
        
        print(f"\n📈 Total alerts generated: {len(self.alerts_history)}")
    
    def data_generator_thread(self):
        """Thread for generating weather data"""
        print("🔄 Starting data generator thread...")
        
        if not self.producer:
            print("❌ Cannot start data generator: Producer not available")
            return
        
        batch_count = 0
        try:
            while True:
                batch_count += 1
                data_batch = self.generate_weather_data()
                
                for data in data_batch:
                    self.producer.send('nepal-weather', data)
                
                self.producer.flush()
                print(f"📤 Batch {batch_count}: Sent {len(data_batch)} records")
                
                time.sleep(15)  # Generate data every 15 seconds
                
        except KeyboardInterrupt:
            print("🛑 Data generator stopped")
        except Exception as e:
            print(f"❌ Data generator error: {e}")
    
    def data_processor_thread(self):
        """Thread for processing data and generating alerts"""
        print("🔄 Starting data processor thread...")
        
        if not self.consumer:
            print("❌ Cannot start data processor: Consumer not available")
            return
        
        try:
            while True:
                # Poll for new messages
                messages = self.consumer.poll(timeout_ms=5000)
                
                if messages:
                    for topic_partition, message_list in messages.items():
                        for message in message_list:
                            try:
                                weather_data = message.value
                                district = weather_data.get('district', 'Unknown')
                                
                                # Process data
                                processed_data = self.calculate_risks(weather_data)
                                self.weather_data[district] = processed_data
                                
                                # Generate alerts
                                alerts = self.analyze_alerts(processed_data)
                                if alerts:
                                    for alert in alerts:
                                        alert_record = {
                                            'timestamp': datetime.now().isoformat(),
                                            'district': district,
                                            'alert': alert,
                                            'weather_data': weather_data
                                        }
                                        self.alerts_history.append(alert_record)
                                        
                                        # Display alert
                                        print(f"\n🚨 ALERT: {district} - {alert['type']} ({alert['severity']})")
                                        print(f"   📝 {alert['message']}")
                                        for action in alert.get('actions', []):
                                            print(f"   • {action}")
                            except Exception as e:
                                print(f"❌ Error processing message: {e}")
                                continue
                
                # Update dashboard every 5 seconds
                self.display_dashboard()
                time.sleep(5)
                
        except KeyboardInterrupt:
            print("🛑 Data processor stopped")
        except Exception as e:
            print(f"❌ Data processor error: {e}")
    
    def start_system(self):
        """Start the complete monitoring system"""
        print("🚀 STARTING NEPAL WEATHER MONITORING SYSTEM")
        print("📍 All-in-One: Data Generation + Processing + Alerts + Dashboard")
        print("⏰ Data updates every 15 seconds")
        print("📊 Real-time dashboard updates every 5 seconds")
        print("-" * 70)
        
        # Start threads
        generator_thread = threading.Thread(target=self.data_generator_thread, daemon=True)
        processor_thread = threading.Thread(target=self.data_processor_thread, daemon=True)
        
        generator_thread.start()
        processor_thread.start()
        
        try:
            # Keep main thread alive
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 System stopped by user")
            print(f"📊 Final stats: {len(self.alerts_history)} alerts generated")

if __name__ == "__main__":
    monitor = NepalWeatherMonitor()
    monitor.start_system()
