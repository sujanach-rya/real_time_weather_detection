# create_docker_compose.py
import os

def create_docker_compose():
    content = """services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
"""
    
    with open('docker-compose.yml', 'w') as f:
        f.write(content)
    
    print("✅ docker-compose.yml created successfully!")
    
    # Verify the file
    with open('docker-compose.yml', 'r') as f:
        lines = f.readlines()
        print("File contents:")
        for i, line in enumerate(lines, 1):
            print(f"{i:2}: {line.rstrip()}")
    
    return True

if __name__ == "__main__":
    create_docker_compose()
