package leon.patmore.kafkareactive

import kotlinx.collections.immutable.ImmutableMap
import kotlinx.collections.immutable.toImmutableMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import org.springframework.validation.annotation.Validated
import java.util.*

@Validated
@ConstructorBinding
@ConfigurationProperties(prefix = "kafka")
class KafkaProperties(private val hosts: String,
                      val topic: String,
                      val commitBatchSize: Int) {

    fun receiverOptions(appName: String): ImmutableMap<String, Any> {
        val props: MutableMap<String, Any> = HashMap()
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = hosts
        props[ConsumerConfig.CLIENT_ID_CONFIG] = "sample-consumer"
        props[ConsumerConfig.GROUP_ID_CONFIG] = appName
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        return props.toImmutableMap()
    }

}
