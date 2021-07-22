package leon.patmore.kafkabatchconsumer

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
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

	@Autowired
	lateinit var consumer: Consumer

	var partitions: Set<TopicPartition>? = null

	@BeforeEach
	fun before() {
		partitions = consumer.getConsumer().assignment()
	}

	@Test
	fun testAddingElementThenCrashing() {
//		val record = ProducerRecord(topic, 0, "key", "value")
//		producer.producer.send(record)
//
//		Thread.sleep(10000)
	}

	fun getConsumerOffset() {
		consumer.getConsumer().position(partitions?.elementAt(0))
	}

}
