package leon.patmore.kafkareactive

import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import reactor.test.StepVerifier
import java.time.Duration

class TestReactor {

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        @JvmStatic
        private val logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }

@Test
fun testDoAfterTerminate() {
    logger.info("Starting test")
    val sch = Schedulers.single()
    val testFlux = Flux.fromArray(intArrayOf(1, 2, 3).toTypedArray())
        .doAfterTerminate { logger.info("Finished processing batch!") }
//        .delaySequence(Duration.ofSeconds(1), sch)
        .doOnNext { logger.info("Done $it")}
        .doAfterTerminate { logger.info("Finished v2")}
    StepVerifier.create(testFlux).expectNextCount(3).verifyComplete()
}

}