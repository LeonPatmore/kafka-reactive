package com.nexmo.kafkalib.retryer.producer

import com.nexmo.kafkalib.retryer.RetryRecord
import reactor.core.publisher.Mono

interface RetryProducer {

    fun retry(record: RetryRecord) : Mono<Void>

}
