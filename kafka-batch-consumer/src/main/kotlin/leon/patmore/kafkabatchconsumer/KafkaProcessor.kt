package leon.patmore.kafkabatchconsumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import reactor.core.publisher.Mono

interface KafkaProcessor {

    fun process(any: ConsumerRecord<String, String>): Mono<Void>

}
