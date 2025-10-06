# csv_data_loader.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pandas as pd

def load_data_from_csv():
    """Load weather data from CSV file"""
    
    spark = SparkSession.builder.getOrCreate()
    
    # Load CSV data
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("weather_data.csv")
    
    # Convert timestamp
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
    
    print("📊 Loaded CSV Data:")
    df.show()
    
    return df

def stream_from_csv_directory():
    """Stream data from a directory containing CSV files"""
    
    spark = SparkSession.builder.getOrCreate()
    
    # Watch a directory for new CSV files
    stream_df = spark.readStream \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .schema(weather_schema) \  # Use the schema we defined earlier
        .csv("data/input/")  # Directory to watch
    
    return stream_df
