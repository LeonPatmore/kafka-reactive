package com.nexmo.kafkalib.processor

import com.nexmo.kafkalib.InternalRecord
import reactor.core.publisher.Mono

interface Processor {

    fun process(record: InternalRecord): Mono<Void>

}
