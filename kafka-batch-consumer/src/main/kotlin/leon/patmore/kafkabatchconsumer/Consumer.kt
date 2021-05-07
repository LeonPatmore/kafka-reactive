package leon.patmore.kafkabatchconsumer

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

@Component
class Consumer(private val sink: Sinks.Many<Flux<ConsumerRecord<String, String>>> = Sinks.many().multicast().onBackpressureBuffer(),
               scheduler: Scheduler = Schedulers.single(),
               val kafkaProcessor: KafkaProcessor,
               subscriber: Scheduler = Schedulers.parallel()) {

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        @JvmStatic
        private val logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }

    private lateinit var consumer: KafkaConsumer<String, String>

    init {
        val props = Properties()
        props["bootstrap.servers"] = "localhost:9092"
        props["group.id"] = "leontest"
        props["enable.auto.commit"] = "false"
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        props["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        props["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        consumer = KafkaConsumer<String, String>(props)
        consumer.subscribe(Collections.singletonList("mytest"))
        scheduler.schedulePeriodically(r {poll()}, 5000, 5000, TimeUnit.MILLISECONDS)

        sink.asFlux().concatMap { r -> r }.flatMap { kafkaProcessor.process(it) }.subscribeOn(subscriber).subscribe()
    }

    private fun r(f: () -> Unit): Runnable = Runnable { f() }

    private fun poll () {
        val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(2))
        logger.info("Consumed [ {} ] records!", records.count())
        val batchFlux = Flux.fromIterable(records).doOnComplete {
            logger.info("Batch finished!")
            records.forEach {
                consumer.commitSync(records)
            }
        }
        sink.tryEmitNext(batchFlux);
    }

}