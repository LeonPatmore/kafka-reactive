package leon.patmore.kafkabatchconsumer

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest
class KafkaBatchConsumerApplicationTests {

	@Value("\${topic}")
	lateinit var topic: String

	@Autowired
	lateinit var producer: TestProducer

	lateinit var consumer: Consumer

	lateinit var partitions: Set<TopicPartition>

	@BeforeAll
	fun before() {
		partitions = consumer.getConsumer().assignment()
	}

	@Test
	fun test() {
		val record = ProducerRecord(topic, 0, "key", "value")
		producer.producer.send(record)

		Thread.sleep(20000)
	}

	fun getConsumerOffset() {
		consumer.getConsumer().position(partitions.elementAt(0))
	}

}
