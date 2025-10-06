# 1. Create the correct docker-compose file
python create_docker_compose.py

# 2. Start Kafka
docker-compose up -d

# 3. Wait 30 seconds
sleep 30

# 4. Create topic
docker-compose exec kafka kafka-topics --create --topic nepal-weather --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# 5. If test passes, run the data feeder
python working_data_feeder.py

