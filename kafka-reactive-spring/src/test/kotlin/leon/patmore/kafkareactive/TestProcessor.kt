package leon.patmore.kafkareactive;

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import java.time.Duration

@Component
class TestProcessor : KafkaProcessor {

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        @JvmStatic
        private val logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }

    override fun process(record: ConsumerRecord<Any, Any>): Mono<ConsumerRecord<Any, Any>> {
        val delay = record.value().toString().toLong()
        return Mono.just(record)
            .doOnNext { logger.info("Starting processing ${it.key()} with delay $delay") }
            .delayElement(Duration.ofMillis(delay))
            .doOnNext { logger.info("Finishing processing ${it.key()}") }
    }

}
