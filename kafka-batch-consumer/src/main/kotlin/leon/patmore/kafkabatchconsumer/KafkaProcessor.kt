package leon.patmore.kafkabatchconsumer

import reactor.core.publisher.Mono

interface KafkaProcessor {

    fun process(any: Any): Mono<Any>

}
