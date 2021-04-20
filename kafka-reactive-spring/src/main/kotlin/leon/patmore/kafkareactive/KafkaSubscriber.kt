package leon.patmore.kafkareactive

import org.springframework.beans.factory.DisposableBean
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.ApplicationListener
import reactor.core.Disposable
import reactor.core.publisher.Flux
import reactor.core.scheduler.Schedulers
import reactor.kafka.receiver.KafkaReceiver
import java.util.logging.Logger

class KafkaSubscriber (kafkaReceiver: KafkaReceiver<Any, Any>,
                       private val kafkaProcessor: KafkaProcessor) : DisposableBean, ApplicationListener<ApplicationReadyEvent> {

    companion object {
        val logger = Logger.getLogger(KafkaSubscriber::class.toString())!!
    }

    private val pipeline: Flux<Void> = kafkaReceiver.receiveAutoAck().concatMap { it.flatMap { record -> kafkaProcessor.process(record).subscribeOn(Schedulers.parallel()) } }
    private lateinit var disposable: Disposable

    private fun start() {
        logger.info("Starting kafka subscriber!")
        this.disposable = this.pipeline.subscribe()
    }

    override fun destroy() {
        this.disposable.dispose()
    }

    override fun onApplicationEvent(event: ApplicationReadyEvent) {
        start()
    }

}