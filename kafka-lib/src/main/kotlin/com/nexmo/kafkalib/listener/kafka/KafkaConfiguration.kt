package com.nexmo.kafkalib.listener.kafka

import io.confluent.parallelconsumer.ParallelConsumerOptions
import io.confluent.parallelconsumer.reactor.ReactorProcessor
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.slf4j.LoggerFactory.getLogger
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.*
import javax.annotation.PostConstruct


@Configuration
class KafkaConfiguration {

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        private val logger = getLogger(javaClass.enclosingClass)
    }

    @Bean
    fun consumerProperties(): Properties {
        val props = Properties()
        props["bootstrap.servers"] = "localhost:9092"
        props["group.id"] = "leontest"
        props["enable.auto.commit"] = "false"
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        props["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        props["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        return props
    }

    @Bean
    fun producerProperties() : Properties {
        val props = Properties()
        props["bootstrap.servers"] = "localhost:9092"
        props["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        props["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        props["transactional.id"] = "leontest"
        return props
    }

    @Bean
    fun getKafkaConsumer(consumerProperties: Properties): KafkaConsumer<String, String> {
        return KafkaConsumer(consumerProperties)
    }

    @Bean
    fun kafkaProducer(producerProperties: Properties) : KafkaProducer<String, String> {
        return KafkaProducer(producerProperties)
    }

    @Bean
    fun parallelConsumerOptions(kafkaConsumer: KafkaConsumer<String, String>,
                                kafkaProducer: KafkaProducer<String, String>): ParallelConsumerOptions<String, String> {
        return ParallelConsumerOptions.builder<String, String>()
                .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
                .commitMode(ParallelConsumerOptions.CommitMode.PERIODIC_TRANSACTIONAL_PRODUCER)
                .consumer(kafkaConsumer)
                .producer(kafkaProducer)
                .build()!!
    }

    @Bean
    fun processor(parallelConsumerOptions: ParallelConsumerOptions<String, String>): ReactorProcessor<String, String> {
        return ReactorProcessor(parallelConsumerOptions)
    }

}