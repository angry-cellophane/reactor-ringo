package org.github.ka.reactor.ringo;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.test.StepVerifier;

import java.time.Duration;

public class BasicUseCasesTest {

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
    void testMonoWithoutTimeout() {
        StepVerifier.create(
                Mono.just(1)
                .timeout(Duration.ofMillis(200), Mono.just(2), scheduler)
        )
        .expectNext(1)
        .expectComplete()
        .verify();
    }

    @Test
    void testMonoReturnsTimeoutResult() {
        StepVerifier.create(
                Monos.monoWithDelay(1, Duration.ofMillis(500))
                        .timeout(Duration.ofMillis(200), Mono.just(2), scheduler)
        )
        .expectNext(2)
        .expectComplete()
        .verify();
    }
}
