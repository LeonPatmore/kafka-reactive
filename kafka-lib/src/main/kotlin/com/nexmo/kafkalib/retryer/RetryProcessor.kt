package com.nexmo.kafkalib.retryer

import com.nexmo.kafkalib.retryer.producer.RetryProducer
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class RetryProcessor(@Autowired private val retryProducer: RetryProducer) {

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        private val logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }

    fun retry(retryRecord: RetryRecord) : Mono<Void> {
        return if (retryRecord.retryCount >= 3) {
            logger.info("Dropping record as it has been retried enough!")
            Mono.empty()
        } else {
            val recordWithIncreasedRetryCount = retryRecord.copy(retryCount = retryRecord.retryCount + 1)
            logger.info("Retrying record for the ${recordWithIncreasedRetryCount.retryCount} time!")
            retryProducer.retry(recordWithIncreasedRetryCount)
        }
    }

}