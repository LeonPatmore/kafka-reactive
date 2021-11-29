package com.nexmo.kafkalib.retryer.consumer.sqs

import com.nexmo.kafkalib.retryer.RetryRecord
import com.nexmo.spring.aws.sqs.ReactiveSqsClient
import com.nexmo.spring.aws.sqs.RecipientHandle
import com.nexmo.spring.aws.sqs.SqsConsumerProperties
import com.nexmo.spring.aws.sqs.SqsMessage
import com.nexmo.spring.aws.sqs.SqsSubscriber
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse
import software.amazon.awssdk.services.sqs.model.Message
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse
import java.util.function.Consumer
import java.util.function.Function

@Service
class RetryConsumerSubscriber(@Autowired private val reactiveSqsClient: ReactiveSqsClient)
    : SqsSubscriber<RetryRecord> {

    private val sqsUrl: String = "http://localhost:9324/queue/default"

    override fun receiveFlux(p0: SqsConsumerProperties?, p1: Function<Message, Mono<SqsMessage<RetryRecord>>>?, p2: Consumer<ReceiveMessageResponse>?): Flux<SqsMessage<RetryRecord>> {
        return reactiveSqsClient.receiveFlux(sqsUrl, RetryRecord::class.java, p0, p1, p2)
    }

    override fun delete(p0: String?): Mono<DeleteMessageResponse> {
        return reactiveSqsClient.delete(sqsUrl, p0)
    }

    override fun deleteBatch(p0: MutableList<out RecipientHandle>?): Mono<ReactiveSqsClient.BatchDeleteResult> {
        return reactiveSqsClient.deleteBatch(sqsUrl, p0)
    }

}
