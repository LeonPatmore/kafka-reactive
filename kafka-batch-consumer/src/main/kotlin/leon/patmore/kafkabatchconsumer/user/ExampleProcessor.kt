package leon.patmore.kafkabatchconsumer.user

import leon.patmore.kafkabatchconsumer.KafkaProcessor
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

@Component
class ExampleProcessor : KafkaProcessor {

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        @JvmStatic
        private val logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }

    override fun process(any: ConsumerRecord<String, String>): Mono<Void> {
        return Mono.just(any).doOnNext{logger.info("Procesing!")}.then()
    }

}
