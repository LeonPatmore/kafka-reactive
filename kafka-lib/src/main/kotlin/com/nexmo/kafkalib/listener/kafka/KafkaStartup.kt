package com.nexmo.kafkalib.listener.kafka

import com.nexmo.kafkalib.InternalProcessor
import com.nexmo.kafkalib.InternalRecord
import io.confluent.parallelconsumer.reactor.ReactorProcessor
import org.slf4j.LoggerFactory.getLogger
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Configuration
import java.util.*
import javax.annotation.PostConstruct

@Configuration
class KafkaStartup(@Autowired private val reactorProcessor: ReactorProcessor<String, String>,
                   @Autowired private val internalProcessor: InternalProcessor) {

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        private val logger = getLogger(javaClass.enclosingClass)
    }

    @PostConstruct
    fun init() {
        logger.info("Starting kafka processor!")
        reactorProcessor.subscribe(setOf("mytest"))
        reactorProcessor.react { record -> internalProcessor.process(InternalRecord(record.key(), record.value(), Collections.emptyMap()))}
    }

}