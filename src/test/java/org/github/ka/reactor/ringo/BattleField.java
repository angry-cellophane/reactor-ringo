package org.github.ka.reactor.ringo;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

@Slf4j
public class BattleField {

    @Test
    void test() {
        Ringo.run(() -> {
            var flux = Flux.just(1, 2, 3)
                    .map(i -> i * 2)
                    .publishOn(Schedulers.boundedElastic())
                    .log("")
                    .filter(i -> i % 2 == 0);

            StepVerifier.create(flux)
                    .expectNext(2)
                    .expectNext(4)
                    .expectNext(6)
                    .expectComplete()
                    .verify();
            System.out.println("done");
        });
    }

    @Test
    void test2() throws InterruptedException {
        int parallelLevel = 1;
        var consumerScheduler = Schedulers.newParallel("consumers", parallelLevel);
        var producerScheduler = Schedulers.newSingle("producer");

        Flux.<Integer>create(sink -> {
            sink.onRequest(n -> {
                log.info(LocalDateTime.now() + "requested " + n + " elements");
                producerScheduler.schedule(() -> {
                    for (int i=0; i<n; i++) {
                        sink.next(1);
                    }
                });
            });
        })
        .limitRate(parallelLevel)
        .parallel(parallelLevel)
        .runOn(consumerScheduler)
        .subscribe(n -> {
            log.info(LocalDateTime.now() + " received number " + n);
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (Exception e) {
                //
            }
        });

        TimeUnit.SECONDS.sleep(30);
    }
}
