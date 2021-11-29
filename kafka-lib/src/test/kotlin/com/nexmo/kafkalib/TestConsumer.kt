package com.nexmo.kafkalib

import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest
@EnableAutoConfiguration
class TestConsumer {

    @Test
    fun test() {
        Thread.sleep(Integer.MAX_VALUE.toLong())
    }

}
