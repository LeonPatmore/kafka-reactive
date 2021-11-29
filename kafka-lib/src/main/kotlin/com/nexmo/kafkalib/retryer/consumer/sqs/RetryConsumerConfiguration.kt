package com.nexmo.kafkalib.retryer.consumer.sqs

import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@EnableConfigurationProperties(RetryConsumerProperties::class)
class RetryConsumerConfiguration
