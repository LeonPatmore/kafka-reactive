package com.nexmo.kafkalib

import com.nexmo.kafkalib.processor.Processor
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.time.Duration

@Service
class TestProcessor: Processor {

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        private val logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }

    override fun process(record: InternalRecord): Mono<Void> {
        return Mono.fromCallable { logger.info("Received record ${record.key} with delay ${record.body}ms") }
                .doOnNext {
                    if (record.body == "-1") {
                        throw Exception("oh no!")
                    }
                }
                .delayElement(Duration.ofMillis(record.body.toLong()))
                .doOnNext { logger.info("Finished record ${record.key}, ${record.body}") }
                .then()
    }

}