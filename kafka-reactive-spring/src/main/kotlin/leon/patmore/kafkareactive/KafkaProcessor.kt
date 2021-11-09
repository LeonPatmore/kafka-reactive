package leon.patmore.kafkareactive

import org.apache.kafka.clients.consumer.ConsumerRecord
import reactor.core.publisher.Mono

interface KafkaProcessor {

    fun process(record: ConsumerRecord<Any, Any>) : Mono<ConsumerRecord<Any, Any>>

}
