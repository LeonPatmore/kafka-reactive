package com.nexmo.kafkalib.retryer

data class RetryRecord(val key: String, val body: String, val headers: Map<String, String>, val retryCount: Int)
