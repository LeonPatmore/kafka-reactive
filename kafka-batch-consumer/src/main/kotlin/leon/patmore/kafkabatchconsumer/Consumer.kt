package leon.patmore.kafkabatchconsumer

import org.apache.kafka.clients.consumer.KafkaConsumer
import reactor.core.publisher.Sinks
import java.util.*

class Consumer ( val sink = Sinks.many().multicast()) {



    fun a() {
        val props = Properties()
        props["bootstrap.servers"] = "localhost:9092"
        props["group.id"] = "test"
        props["enable.auto.commit"] = "true"
        props["auto.commit.interval.ms"] = "1000"
        props["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        props["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        val consumer: KafkaConsumer<String, String> = KafkaConsumer<String, String>(props)
        consumer.poll()
    }

    fun poll

}