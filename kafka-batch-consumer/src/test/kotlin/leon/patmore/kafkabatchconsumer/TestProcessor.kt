package leon.patmore.kafkabatchconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.time.Duration

@Component
class TestProcessor : KafkaProcessor {

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        @JvmStatic
        private val logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }

    override fun process(any: ConsumerRecord<String, String>): Mono<Void> {
        val delay = any.value().toLong()
        return Mono.just(any)
            .doOnNext { logger.info("Starting processing ${it.key()} with delay $delay") }
            .delayElement(Duration.ofMillis(delay))
            .doOnNext { logger.info("Finishing processing ${it.key()}") }
            .then()
    }

}
