package com.nexmo.kafkalib

import com.nexmo.kafkalib.processor.Processor
import com.nexmo.kafkalib.retryer.RetryProcessor
import com.nexmo.kafkalib.retryer.RetryRecord
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class InternalProcessor(
    private val processor: Processor,
    private val retryProcessor: RetryProcessor
) {

    fun process(record: InternalRecord): Mono<Void> {
        return processor.process(record)
            .onErrorResume { retryProcessor.retry(RetryRecord(record.key, record.body, record.headers, record.retryCount)) }
    }

}