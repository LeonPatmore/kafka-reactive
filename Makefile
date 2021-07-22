ifeq ($(OS),Windows_NT)
    detected_OS := Windows
else
    detected_OS := $(shell uname)
endif

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

setJavaEnvVarsWindows: ## Sets the env vars for Java for Windows
	echo "set JAVA_HOME=C:\Users\Leon\Downloads\jdk-11" > .env; \
	echo "set PATH=%PATH%;C:\Users\Leon\Downloads\jdk-11\bin" >> .env

setJavaEnvVars: ## Set the env vars for Java
	ifeq ($(OS),Windows_NT)
		$(MAKE) setJavaEnvVarsWindows

startKafka:  ## Starts local kafka for testing
	docker-compose -f kafka-compose.yml up -d

testKafka: ## Tests if Kafka is running correctly
	docker run -it --network=host edenhill/kafkacat:1.6.0 -b localhost:9092 -L

produce: ## Produces a test message
	docker run -it --network=host --entrypoint "/bin/sh" edenhill/kafkacat:1.6.0 -c "echo Hello | kafkacat -b localhost:9092 -P -t mytest"

offset: ## Get the offset for the test topic
	docker run evpavel/kt kt group -brokers 192.168.99.100:9092 -group leontest -topic mytest
