package leon.patmore.kafkareactive

import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import java.util.*

@Configuration
@EnableConfigurationProperties(KafkaProperties::class)
class KafkaConfiguration {

    @Value("\${spring.application.name}")
    lateinit var appName: String;

    /**
     * Creates the receiver options for this kafka consumer.
     */
    @Bean
    fun receiverOptions(kafkaProperties: KafkaProperties) : ReceiverOptions<Any, Any> {
        return ReceiverOptions.create<Any, Any>(kafkaProperties.receiverOptions(appName))
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