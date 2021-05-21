package org.github.ka.reactor.ringo;

import lombok.SneakyThrows;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class Utils {
    static <T> Mono<T> monoWithDelay(T result, Duration delay) {
        return Mono.create(s -> {
            sleep(delay.toMillis());
            s.success(result);
        });
    }

    static <T> Flux<T> fluxWithDelay(Duration delay, T ... result) {
        return Flux.create(s -> {
            sleep(delay.toMillis());
            for (T r : result) {
                s.next(r);
            }
        });
    }

    @SneakyThrows
    private static void sleep(long millis) {
        Thread.sleep(millis);
    }
}
