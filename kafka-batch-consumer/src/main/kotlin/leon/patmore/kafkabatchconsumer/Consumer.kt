package leon.patmore.kafkabatchconsumer

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
class Consumer(val sink: Sinks.Many<Flux<ConsumerRecord<String, String>>> = Sinks.many().multicast().onBackpressureBuffer(),
               scheduler: Scheduler = Schedulers.single()) {

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        @JvmStatic
        private val logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }

    private lateinit var consumer: KafkaConsumer<String, String>

    init {
        val props = Properties()
        props["bootstrap.servers"] = "localhost:9092"
        props["group.id"] = "test"
        props["enable.auto.commit"] = "true"
        props["auto.commit.interval.ms"] = "1000"
        props["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        props["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        consumer = KafkaConsumer<String, String>(props)
        scheduler.schedulePeriodically(r {poll()}, 1000, 1000, TimeUnit.MILLISECONDS)
    }

    private fun r(f: () -> Unit): Runnable = Runnable { f() }

    private fun poll () {
        val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(2))
        logger.info("Consumed [ {} ] records!", consumer)
        val batchFlux = Flux.fromIterable(records).doOnComplete {
            logger.info("Batch finished!")
        }
        sink.tryEmitNext(batchFlux);
    }

}