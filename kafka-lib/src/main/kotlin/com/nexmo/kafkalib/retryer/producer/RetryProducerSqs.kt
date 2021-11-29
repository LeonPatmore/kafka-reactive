package com.nexmo.kafkalib.retryer.producer

import com.nexmo.kafkalib.retryer.RetryRecord
import com.nexmo.spring.aws.sqs.MessageRequest
import com.nexmo.spring.aws.sqs.ReactiveSqsClient
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class RetryProducerSqs(@Autowired private val reactiveSqsClient: ReactiveSqsClient) : RetryProducer {

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        private val logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }

    override fun retry(record: RetryRecord): Mono<Void> {
        return reactiveSqsClient.send(MessageRequest.to("http://localhost:9324/queue/default")
                                              .withBody(record)
                                              .withDelaySeconds(5))
                .doOnNext {
                    logger.info("Send to SQS with response $it")
                }
                .then()
    }

}
