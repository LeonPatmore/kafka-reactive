package leon.patmore.kafkareactive

import org.apache.kafka.clients.consumer.ConsumerRecord
import reactor.core.publisher.Mono
import java.util.logging.Logger

class KafkaProcessor {

    companion object {
        val logger = Logger.getLogger(KafkaProcessor::class.toString())!!
    }

    fun process(record: ConsumerRecord<Any, Any>) : Mono<Void> {
        return Mono.just(record).doOnNext { logger.info(it.value().toString()) }.then()
    }

}