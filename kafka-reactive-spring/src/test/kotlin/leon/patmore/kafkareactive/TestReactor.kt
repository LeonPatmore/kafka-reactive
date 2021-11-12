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
        val sch = Schedulers.newBoundedElastic(100, 100, "a")
        val testFlux = Flux.fromArray(intArrayOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12).toTypedArray())
            .doAfterTerminate { logger.info("Finished processing batch!") }
            .parallel()
            .doOnNext { logger.info("Done $it")}
            .flatMap { Mono.fromCallable { Thread.sleep(1000) } }
            .sequential()
            .doAfterTerminate { logger.info("Finished v2")}
            .subscribeOn(sch)
        StepVerifier.create(testFlux).expectNextCount(12).verifyComplete()
    }

}