package leon.patmore.kafkabatchconsumer

import org.apache.kafka.clients.consumer.ConsumerRecord
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler

interface KafkaProcessor {

    fun process(any: ConsumerRecord<String, String>, s: Scheduler): Mono<Void>

}
