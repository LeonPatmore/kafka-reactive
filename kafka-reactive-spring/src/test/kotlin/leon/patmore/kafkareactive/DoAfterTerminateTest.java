package leon.patmore.kafkareactive;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;

public class DoAfterTerminateTest {

    @Test
    public void test() {
        StepVerifier.create(Flux.fromArray(new String[]{"hi", "how are you", "bye"})
                                .doAfterTerminate(() -> System.out.println("DONE!"))
                                .delaySequence(Duration.ofSeconds(2))
                                .doOnNext(str -> System.out.println("Finished " + str)))
                    .expectNextCount(3)
                    .verifyComplete();
    }

    @Test
    public void test2() {
        StepVerifier.create(Flux.fromArray(new String[]{"hi", "how are you", "bye"})
                                .delaySequence(Duration.ofSeconds(2))
                                .doOnNext(str -> System.out.println("Finished " + str))
                                .doAfterTerminate(() -> System.out.println("DONE!")))
                    .expectNextCount(3)
                    .verifyComplete();
    }

}
