package com.nexmo.kafkalib

data class InternalRecord(val key: String, val body: String, val headers: Map<String, String>, val retryCount: Int = 0)
