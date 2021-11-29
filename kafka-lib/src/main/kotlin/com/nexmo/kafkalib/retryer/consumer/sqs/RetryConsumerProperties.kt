package com.nexmo.kafkalib.retryer.consumer.sqs

import com.nexmo.spring.aws.sqs.SqsConsumerProperties
import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("retry.consumer")
class RetryConsumerProperties : SqsConsumerProperties()
