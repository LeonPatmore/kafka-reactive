package leon.patmore.kafkabatchconsumer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.springframework.stereotype.Component
import java.util.*

@Component
class TestProducer {

    final val producer: KafkaProducer<String, String>

    init {
        val props = Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9092"
        props[ProducerConfig.CLIENT_ID_CONFIG] = "test-client"
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = "org.apache.kafka.common.serialization.StringSerializer"
        props[ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG] = 5000;
        producer = KafkaProducer<String, String>(props);
    }

}