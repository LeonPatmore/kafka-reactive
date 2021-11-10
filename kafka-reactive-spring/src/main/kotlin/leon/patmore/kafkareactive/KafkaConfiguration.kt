package leon.patmore.kafkareactive

import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import java.util.*
import java.util.logging.Logger

@Configuration
@EnableConfigurationProperties(KafkaProperties::class)
class KafkaConfiguration(@Value("\${spring.application.name}") private val appName: String) {

    companion object {
        private val logger = Logger.getLogger(KafkaConfiguration::class.toString())!!
    }

    @Bean
    fun receiverOptions(kafkaProperties: KafkaProperties) : ReceiverOptions<Any, Any> {
        logger.info("Using commit batch size [ ${kafkaProperties.commitBatchSize} ]")
        return ReceiverOptions.create<Any, Any>(kafkaProperties.receiverOptions(appName))
                .subscription(Collections.singleton(kafkaProperties.topic))
    }

    @Bean
    fun receiver(receiverOptions: ReceiverOptions<Any, Any>) : KafkaReceiver<Any, Any> {
        return KafkaReceiver.create(receiverOptions)
    }

    @Bean
    fun subscriber(kafkaReceiver: KafkaReceiver<Any, Any>, kafkaProcessor: KafkaProcessor) : KafkaSubscriber {
        return KafkaSubscriber(kafkaReceiver, kafkaProcessor)
    }

}
