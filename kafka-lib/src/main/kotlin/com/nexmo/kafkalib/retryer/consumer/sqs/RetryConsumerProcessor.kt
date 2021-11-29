package com.nexmo.kafkalib.retryer.consumer.sqs

import com.nexmo.kafkalib.InternalProcessor
import com.nexmo.kafkalib.InternalRecord
import com.nexmo.kafkalib.retryer.RetryRecord
import com.nexmo.spring.aws.sqs.SqSProcessor
import com.nexmo.spring.aws.sqs.SqsConsumerProperties
import com.nexmo.spring.aws.sqs.SqsMessage
import com.nexmo.spring.aws.sqs.SqsSubscriber
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class RetryConsumerProcessor(@Autowired private val sqsConsumerProperties: SqsConsumerProperties,
                             @Autowired private val sqsSubscriber: SqsSubscriber<RetryRecord>,
                             @Autowired private val internalProcessor: InternalProcessor)
    : SqSProcessor<RetryRecord>(sqsConsumerProperties, sqsSubscriber) {

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        private val logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }

    override fun process(sqsMessage: SqsMessage<RetryRecord>?): Mono<ProcessingResult> {
        return Mono.just(sqsMessage!!)
                .doOnNext {
                    logger.info("Received a retry message in SQS!")
                }
                .flatMap {
                    internalProcessor.process(InternalRecord(sqsMessage.body.key,
                                                             sqsMessage.body.body,
                                                             sqsMessage.body.headers,
                                                             sqsMessage.body.retryCount))
                }
                .thenReturn(ProcessingResult.ACKNOWLEDGE)
    }


}