from kafka_utils import KafkaUtils

bootstrap_server = "localhost:9092"
topic = "mytest"

kafka_utils = KafkaUtils([bootstrap_server], topic)
