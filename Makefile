help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

startKafka:  ## Starts local kafka for testing
	docker-compose -f kafka-compose.yml up -d

testKafka: ## Tests if Kafka is running correctly
	docker run -it --network=host edenhill/kafkacat:1.6.0 -b localhost:9092 -L

produce: ## Produces a test message
	docker run -it --network=host --entrypoint "/bin/sh" edenhill/kafkacat:1.6.0 -c "echo Hello | kafkacat -b localhost:9092 -P -t mytest"

offset: ## Get the offset for the test topic
	docker run evpavel/kt kt group -brokers 192.168.99.100:9092 -group leontest -topic mytest
