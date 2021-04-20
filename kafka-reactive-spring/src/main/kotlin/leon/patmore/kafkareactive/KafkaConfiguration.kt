package leon.patmore.kafkareactive

import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import java.time.Duration
import java.util.*
import java.util.logging.Logger

@Configuration
@EnableConfigurationProperties(KafkaProperties::class)
class KafkaConfiguration {

    companion object {
        val logger = Logger.getLogger(KafkaProcessor::class.toString())!!
    }

    @Value("\${spring.application.name}")
    lateinit var appName: String;

    /**
     * Creates the receiver options for this kafka consumer.
     */
    @Bean
    fun receiverOptions(kafkaProperties: KafkaProperties) : ReceiverOptions<Any, Any> {
        logger.info("Using commit batch size [ ${kafkaProperties.commitBatchSize} ]")
        return ReceiverOptions.create<Any, Any>(kafkaProperties.receiverOptions(appName))
                .commitBatchSize(kafkaProperties.commitBatchSize)
                .commitInterval(Duration.ZERO)
                .subscription(Collections.singleton(kafkaProperties.topic))
    }

    /**
     * Creates the kafka receiver for this consumer.
     */
    @Bean
    fun receiver(receiverOptions: ReceiverOptions<Any, Any>) : KafkaReceiver<Any, Any> {
        return KafkaReceiver.create(receiverOptions)
    }

    @Bean
    fun processor() : KafkaProcessor {
        return KafkaProcessor()
    }

    @Bean
    fun subscriber(kafkaReceiver: KafkaReceiver<Any, Any>, kafkaProcessor: KafkaProcessor) : KafkaSubscriber {
        return KafkaSubscriber(kafkaReceiver, kafkaProcessor)
    }

}
