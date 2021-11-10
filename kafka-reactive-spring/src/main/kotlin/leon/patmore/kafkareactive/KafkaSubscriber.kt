package leon.patmore.kafkareactive

import org.springframework.beans.factory.DisposableBean
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationListener
import reactor.core.Disposable
import reactor.kafka.receiver.KafkaReceiver
import java.util.logging.Logger
import kotlin.math.log

class KafkaSubscriber (kafkaReceiver: KafkaReceiver<Any, Any>,
                       private val kafkaProcessor: KafkaProcessor) : DisposableBean, ApplicationListener<ApplicationReadyEvent> {

    companion object {
        private val logger = Logger.getLogger(KafkaSubscriber::class.toString())!!
    }

//    private val pipeline: Flux<Void> = kafkaReceiver.receiveAutoAck()
//            .concatMap { it.flatMap { record -> kafkaProcessor.process(record).subscribeOn(Schedulers.parallel()) } }

//    private val pipeline: Flux<ConsumerRecord<Any, Any>> = kafkaReceiver
//            .receiveAutoAck()
//            .flatMap { batchFlux -> batchFlux.flatMap { kafkaProcessor.process(it) } }

    private val disposable: Disposable = kafkaReceiver
            .receiveAutoAck()
            .flatMap{r -> r.flatMap { kafkaProcessor.process(it) }}
            .subscribe{r -> logger.info { "Received: $r" }}

//        private val disposable: Disposable = kafkaReceiver
//            .receiveAutoAck()
//            .map{ r -> r.flatMap { kafkaProcessor.process(it) }}
//            .doOnNext { it.subscribe() }
//                .subscribe()

//    private lateinit var disposable: Disposable

    private fun start() {
        logger.info("Starting kafka subscriber!")
//        this.disposable = this.pipeline.subscribe()
    }

    override fun destroy() {
        this.disposable.dispose()
    }

    override fun onApplicationEvent(event: ApplicationReadyEvent) {
        start()
    }

}