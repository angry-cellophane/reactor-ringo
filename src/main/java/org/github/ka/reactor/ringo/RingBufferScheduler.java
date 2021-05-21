package org.github.ka.reactor.ringo;

import lombok.extern.slf4j.Slf4j;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Consumer;

@Slf4j
public class RingBufferScheduler implements Scheduler {

    private static final int DEFAULT_BUCKET_SIZE = 10_000;
    private static final int BUCKET_TIME_WINDOW_MS = 64; // in ms
    private static final int BUCKET_TIME_WINDOW_POWER_OF_2 = 5;

    private final AtomicReferenceArray<Runnable>[] buckets;
    private final AtomicInteger[] indices;
    private final Thread cleaner;
    private final Consumer<Runnable> disposeTasks;

    private volatile boolean isShuttingDown = false;
    private volatile int index = 0;

    @SuppressWarnings("unchecked")
    RingBufferScheduler(Duration windowSize, int bucketSize, Consumer<Runnable> disposeTasks) {
        /*
            it guarantees to delete element from bucket within (bucket left bound + 100ms)
            windowSize is the right bound of bucket, need to add 100ms to make it left bound of another cell
            Also adding + 1 bucket for obsolete tasks that are out of window and should be deleted. It is always buckets[current index - 1]
         */
        int cells = ((int)((windowSize.toMillis() + BUCKET_TIME_WINDOW_MS) >> BUCKET_TIME_WINDOW_POWER_OF_2)) + 1;
        log.debug("number of cells " + cells);
        this.buckets = new AtomicReferenceArray[cells];
        this.indices = new AtomicInteger[cells];
        for (int i = 0; i < cells; i++) {
            this.buckets[i] = new AtomicReferenceArray<>(bucketSize);
            this.indices[i] = new AtomicInteger(0);
        }

        this.cleaner = newCleaner();
        this.disposeTasks = disposeTasks;
    }

    private Thread newCleaner() {
        var cleaner = new Thread(this::cleaner);
        cleaner.setDaemon(true);
        cleaner.setPriority(Thread.NORM_PRIORITY);
        cleaner.setName("ring-buffer-scheduler-cleaner");

        return cleaner;
    }

    private void cleaner() {
        long prev = System.currentTimeMillis() >> BUCKET_TIME_WINDOW_POWER_OF_2;
        while (!isShuttingDown) {
            try {
                var currentBucket = index;
                var now = System.currentTimeMillis() >> BUCKET_TIME_WINDOW_POWER_OF_2;
                int diff = (int) (now - prev);
                if (diff != 0) {
                    if (diff > 1) {
                        log.debug("unexpected state: one bucket skipped in cleaner, diff is " + (diff));
                    }
                    index = (currentBucket + diff) % indices.length; // only this thread updates the value
                    prev = now;
                }
                int prevBucket = currentBucket == 0 ? buckets.length - 1
                        : (currentBucket - 1);
                int limit = indices[prevBucket].get();
                if (limit != 0) {
                    for (int i = 0; i < limit; i++) {
                        var runnable = buckets[prevBucket].get(i);
                        if (runnable != null) {
                            disposeTasks.accept(runnable);
                            log.debug("delete element in bucket " + prevBucket + ", idx = " + i);
                        }
                        buckets[prevBucket].set(i, null);
                    }
                    indices[prevBucket].set(0);
                }
            } catch (Exception e) {
                log.error("error in picker", e);
            }
        }
    }


    @Override
    public Disposable schedule(Runnable task) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
        var delayInMillis = unit.toMillis(delay);
        log.debug("adding element with delay " + delayInMillis);

        var bucketNumber = (delayInMillis >> BUCKET_TIME_WINDOW_POWER_OF_2);
        log.debug("bucketNumber " + bucketNumber);
        if (bucketNumber >= buckets.length) {
            log.debug("offered " + delayInMillis + " in bucket " + bucketNumber);
            throw new RuntimeException("offered task is outside of interval");
        }
        var currentIdx = index;
        log.debug("currentIdx " + currentIdx);
        bucketNumber = (currentIdx + bucketNumber) % buckets.length;
        final int bucketIdx = (int) bucketNumber;

        var idx = indices[bucketIdx].getAndIncrement();
        if (idx >= buckets[bucketIdx].length()) {
            throw new RuntimeException("reached bucket limit of " + buckets[bucketIdx].length());
        }
        var chm = new ConcurrentHashMap<Runnable, Void>(10_000, 1.0f, 512);

        buckets[bucketIdx].set(idx, task);
        log.debug("added element in bucket " + bucketIdx + ", idx = " + idx);

        return () -> {
            log.debug("dispose element in bucket " + bucketIdx + ", idx = " + idx);
            buckets[bucketIdx].set(idx, null);
        };
    }

    @Override
    public Disposable schedulePeriodically(Runnable task, long initialDelay, long period, TimeUnit unit) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public long now(TimeUnit unit) {
        return System.currentTimeMillis();
    }

    @Override
    public Worker createWorker() {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void dispose() {
        isShuttingDown = true;
    }

    @Override
    public void start() {
        if (cleaner.isAlive()) return;

        cleaner.start();
    }

    public static Scheduler create(Duration windowSize) {
        return create(windowSize, DEFAULT_BUCKET_SIZE);
    }

    public static Scheduler create(Duration windowSize, int bucketSize) {
        var scheduler =  new RingBufferScheduler(windowSize, bucketSize, Runnable::run);
        scheduler.start();
        return scheduler;
    }

    public static Scheduler create(Duration windowSize, ExecutorService disposeTasks) {
        return create(windowSize, disposeTasks, DEFAULT_BUCKET_SIZE);
    }

    public static Scheduler create(Duration windowSize, ExecutorService disposeTasks, int bucketSize) {
        var scheduler = new RingBufferScheduler(windowSize, bucketSize, disposeTasks::submit);
        scheduler.start();
        return scheduler;
    }
}
