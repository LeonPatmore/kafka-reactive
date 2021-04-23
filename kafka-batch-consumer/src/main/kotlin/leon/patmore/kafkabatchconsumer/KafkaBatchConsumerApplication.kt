package leon.patmore.kafkabatchconsumer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaBatchConsumerApplication

fun main(args: Array<String>) {
	runApplication<KafkaBatchConsumerApplication>(*args)
}
