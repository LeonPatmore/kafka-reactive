package com.nexmo.kafkalib

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaLibApplication

fun main(args: Array<String>) {
	runApplication<KafkaLibApplication>(*args)
}
