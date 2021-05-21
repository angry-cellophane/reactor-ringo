package org.github.ka.reactor.ringo;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class FluxBasicUseCasesTest {

    static Scheduler scheduler;

    @BeforeAll
    static void setupScheduler() {
        scheduler = RingBufferScheduler.create(Duration.ofMillis(500));
    }

    @AfterAll
    static void cleanup() {
        if (scheduler != null) scheduler.dispose();
    }

    @Test
    void testOneElementNoTimeout() {
        StepVerifier.create(
                Flux.just(1)
                .timeout(Duration.ofMillis(200), Flux.just(2), scheduler)
        )
        .expectNext(1)
        .expectComplete()
        .verify();
    }

    @Test
    void test10ElementsNoTimeout() {
        var nums = IntStream.range(1, 11).mapToObj(i -> i).collect(Collectors.toList());
        StepVerifier.create(
                Flux.fromIterable(nums)
                        .timeout(Duration.ofMillis(200), Flux.just(-1), scheduler)
        )
        .expectNextSequence(nums)
        .expectComplete()
        .verify();
    }

    @Test
    void test3ElementsAnd3Timeouts() {
        StepVerifier.create(
                Flux.fromIterable(List.of(1,2,3))
                        .concatWith(
                                Utils.fluxWithDelay(Duration.ofMillis(200), 4,5,6)
                        )
                        .timeout(Duration.ofMillis(50), Flux.just(-1,-1,-1), scheduler)
        )
        .expectNextSequence(List.of(1,2,3,-1,-1,-1))
        .expectComplete()
        .verify();
    }
}
