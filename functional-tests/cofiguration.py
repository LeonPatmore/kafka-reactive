from kafka_utils import KafkaUtils

bootstrap_server = "localhost:9092"
topic = "mytest"
group_id = "leontest"

kafka_utils = KafkaUtils([bootstrap_server], topic, group_id)
