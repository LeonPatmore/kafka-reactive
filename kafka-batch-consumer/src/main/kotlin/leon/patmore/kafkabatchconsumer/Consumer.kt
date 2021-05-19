package leon.patmore.kafkabatchconsumer

import org.apache.kafka.clients.consumer.*
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Sinks
import reactor.core.scheduler.Scheduler
import reactor.core.scheduler.Schedulers
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.collections.HashMap

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

        sink.asFlux()
                .concatMap { r -> r }
                .flatMap { kafkaProcessor.process(it) }
                .doOnError{ err ->
                    run {
                        logger.info("Could not process record due to $err")
                    }
//                    Send to DLQ or send back to original topic.
                }
                .subscribeOn(subscriber).subscribe()
    }

    private fun r(f: () -> Unit): Runnable = Runnable { f() }

    private fun poll () {
        val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(2))
        logger.info("Consumed [ {} ] records!", records.count())
        val batchFlux = Flux.fromIterable(records).doOnComplete {
            logger.info("Batch finished!")
            val offsetMap = HashMap<TopicPartition, OffsetAndMetadata>()
            records.forEach {
                val partition = TopicPartition(it.topic(), it.partition())
                val currentOffset = offsetMap.getOrDefault(partition, null)
                val currentOffsetNum: Long = if (Objects.isNull(currentOffset)) {
                    -1
                } else {
                    currentOffset?.offset()!!
                }
                val newOffsetNum: Long = if (currentOffsetNum < it.offset()) {
                    it.offset() + 1
                } else {
                    currentOffsetNum
                }
                offsetMap[partition] = OffsetAndMetadata(newOffsetNum)
            }
            consumer.commitSync(offsetMap)
        }
        sink.tryEmitNext(batchFlux);
    }

}