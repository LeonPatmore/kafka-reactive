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
class Consumer(private val sink: Sinks.Many<Iterable<ConsumerRecord<String, String>>> = Sinks.many().multicast().onBackpressureBuffer(),
               private val scheduler: Scheduler = Schedulers.single(),
               val kafkaProcessor: KafkaProcessor) {

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

        consume().subscribeOn(scheduler).subscribe()
    }

    private fun consume(): Flux<Void> {
        return sink.asFlux().flatMap {
                Flux.fromIterable(it)
                        .flatMap { record -> kafkaProcessor.process(record, scheduler) }
                        .doOnError { err ->
                            run {
                                logger.info("Could not process record due to $err")
                            }
//                    Send to DLQ or send back to original topic.
                        }
                        .doOnComplete {commitBatch(it)}
            }
    }

    private fun commitBatch(batch: Iterable<ConsumerRecord<String, String>>) {
        logger.info("Batch finished!")
        val offsetMap = HashMap<TopicPartition, OffsetAndMetadata>()
        batch.forEach {
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

    private fun r(f: () -> Unit): Runnable = Runnable { f() }

    private fun poll() {
        val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(2))
        sink.tryEmitNext(records)
    }

    fun getConsumer(): KafkaConsumer<String, String> {
        return consumer
    }

}
