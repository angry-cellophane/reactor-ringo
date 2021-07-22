package org.github.ka.reactor.ringo;

import reactor.core.publisher.Hooks;
import reactor.core.publisher.Wrappers;

import java.util.concurrent.Callable;

public class Ringo {

    public static void enable() {
        Hooks.onEachOperator("ringo", Wrappers.uberWrapper());
    }

    public static void disable() {
        Hooks.resetOnEachOperator("ringo");
    }

    public static <T> T run(Callable<T> callable) throws Exception {
        enable();
        try {
            return callable.call();
        } finally {
            disable();
        }
    }

    public static void run(Runnable runnable) {
        enable();
        try {
            runnable.run();
        } finally {
            disable();
        }
    }
}
