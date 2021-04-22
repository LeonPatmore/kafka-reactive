package leon.patmore.kafkareactive

import org.apache.kafka.clients.consumer.ConsumerRecord
import reactor.core.publisher.Mono
import java.time.Duration
import java.util.logging.Logger

class KafkaProcessor {

    companion object {
        val logger = Logger.getLogger(KafkaProcessor::class.toString())!!
    }

    /**
     * For testing scheduler.
     */
    fun process(record: ConsumerRecord<Any, Any>) : Mono<ConsumerRecord<Any, Any>> {
        return Mono.just(record)
                .doOnNext { logger.info("Started " + it.value().toString()) }
                .delayElement(Duration.ofSeconds(30))
                .doOnNext{ logger.info("Finished " + it.value().toString()) }
    }

}