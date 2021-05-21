package org.github.ka.reactor.ringo;

import lombok.SneakyThrows;
import reactor.core.publisher.Mono;

import java.time.Duration;

public class Utils {
    static <T> Mono<T> monoWithDelay(T result, Duration delay) {
        return Mono.create(s -> {
            sleep(delay.toMillis());
            s.success(result);
        });
    }

    @SneakyThrows
    private static void sleep(long millis) {
        Thread.sleep(millis);
    }
}
