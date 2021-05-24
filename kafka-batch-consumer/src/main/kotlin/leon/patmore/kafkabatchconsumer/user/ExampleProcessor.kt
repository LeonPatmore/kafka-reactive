package leon.patmore.kafkabatchconsumer.user

import leon.patmore.kafkabatchconsumer.KafkaProcessor
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.core.scheduler.Scheduler
import java.time.Duration

@Component
class ExampleProcessor : KafkaProcessor {

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        @JvmStatic
        private val logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }

    override fun process(any: ConsumerRecord<String, String>, s: Scheduler): Mono<Void> {
        return Mono.just(any)
                .doOnNext{logger.info("Starting procesing ${it.value()}")}
                .delayElement(Duration.ofSeconds(5), s)
                .doOnNext{logger.info("Finishing procesing ${it.value()}")}
                .then()
    }

}
