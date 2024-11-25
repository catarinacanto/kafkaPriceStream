KAFKA_CONTAINER = kafka
KAFKA_TOPIC_1 = etf-prices  # Topic for the consumer
KAFKA_TOPIC_2 = aggregated-etf-prices  # Topic for the producerKAFKA_BROKER = localhost:9092
KAFKA_BROKER = localhost:9092

# Start the services in the background (detached mode)
up:
	docker-compose up -d

# Stop and remove the services
down:
	docker-compose down

# Open a bash shell in the Kafka container
bash:
	@docker exec -it $(KAFKA_CONTAINER) bash

# Create both Kafka topics (etf-prices and aggregated-etf-prices)
create-topics:
	@docker exec -it $(KAFKA_CONTAINER) kafka-topics.sh --create --topic $(KAFKA_TOPIC_1) --bootstrap-server $(KAFKA_BROKER) --partitions 1 --replication-factor 1
	@docker exec -it $(KAFKA_CONTAINER) kafka-topics.sh --create --topic $(KAFKA_TOPIC_2) --bootstrap-server $(KAFKA_BROKER) --partitions 1 --replication-factor 1
	@echo "Topics $(KAFKA_TOPIC_1) and $(KAFKA_TOPIC_2) created successfully!"

# List all Kafka topics
list-topics:
	@docker exec -it $(KAFKA_CONTAINER) kafka-topics.sh --list --bootstrap-server $(KAFKA_BROKER)

# Check the messages in etf-prices
check-etf-prices:
	@docker exec -it $(KAFKA_CONTAINER) kafka-console-consumer.sh --bootstrap-server $(KAFKA_BROKER) --topic $(KAFKA_TOPIC_1) --from-beginning
	@echo "Checked messages in topic $(KAFKA_TOPIC_1)."

# Check the messages in etf-prices
check-aggregated-etf-prices:
	@docker exec -it $(KAFKA_CONTAINER) kafka-console-consumer.sh --bootstrap-server $(KAFKA_BROKER) --topic $(KAFKA_TOPIC_2) --from-beginning
	@echo "Checked messages in topic $(KAFKA_TOPIC_1)."

# Produce a test message to the `etf-prices` topic
produce-message:
	echo '{"etf":"ETF12","isin":"US123456780","timestamp":"2024-11-25T00:00:00Z","course":1533.0}' | docker exec -i $(KAFKA_CONTAINER) kafka-console-producer.sh --broker-list $(KAFKA_BROKER) --topic $(KAFKA_TOPIC_1)
	@echo "Test message produced to topic $(KAFKA_TOPIC_1)."

# Delete etf-prices topic
delete-etf-prices:
	@docker exec -it $(KAFKA_CONTAINER) kafka-topics.sh --delete --topic $(KAFKA_TOPIC_1) --bootstrap-server $(KAFKA_BROKER)
	@echo "Topic $(KAFKA_TOPIC_1) deleted."

# Delete aggregated-etf-prices topic
delete-aggregated-etf-prices:
	@docker exec -it $(KAFKA_CONTAINER) kafka-topics.sh --delete --topic $(KAFKA_TOPIC_2) --bootstrap-server $(KAFKA_BROKER)
	@echo "Topic $(KAFKA_TOPIC_2) deleted."