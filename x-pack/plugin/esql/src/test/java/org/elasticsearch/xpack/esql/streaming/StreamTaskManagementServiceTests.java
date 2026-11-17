/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

/**
 * The counterpart of {@code AsyncTaskManagementServiceTests}, for {@code wait_timeout}.
 * <p>
 * Async waits for completion. The stream GET never does: it returns what {@code getPages} returns, and {@code wait_timeout} only bounds
 * how long it waits when there are no rows yet.
 */
public class StreamTaskManagementServiceTests extends AbstractStreamTestCase {

    private static final long CHUNK = 100;

    private PlainActionFuture<StreamPages> wait(CancellableTask task, StreamStorage storage, String id, long offset, TimeValue timeout) {
        PlainActionFuture<StreamPages> future = new PlainActionFuture<>();
        StreamTaskManagementService.addCompletionListener(threadPool, task, storage, id, offset, CHUNK, timeout, future);
        return future;
    }

    private PlainActionFuture<StreamPages> wait(String id, long offset, TimeValue timeout) {
        return wait(newTask(), store, id, offset, timeout);
    }

    private static long elapsedMillis(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    /** Runs {@code action} on the thread pool after {@code delayMillis}, like a query that is still adding pages. */
    private void later(long delayMillis, Runnable action) {
        threadPool.schedule(action, TimeValue.timeValueMillis(delayMillis), threadPool.generic());
    }

    public void testReturnBeforeTimeout() {
        Writer writer = createStream("s");
        later(100, () -> writer.append(5));
        long start = System.nanoTime();
        try (StreamPages chunk = safeGet(wait("s", 0, TimeValue.timeValueSeconds(30)))) {
            assertThat("returns as soon as a poll sees rows, not at the deadline", elapsedMillis(start), lessThan(20_000L));
            assertThat(chunk.rows(), equalTo(5L));
            assertThat(chunk.metadata().status(), equalTo(StreamMetadata.Status.RUNNING));
        }
    }

    public void testReturnAfterTimeout() {
        Writer writer = createStream("s");
        long start = System.nanoTime();
        try (StreamPages chunk = safeGet(wait("s", 0, TimeValue.timeValueMillis(300)))) {
            long elapsed = elapsedMillis(start);
            // a timeout is not an error: the running result comes back, with no rows
            assertThat(chunk.pages(), equalTo(List.of()));
            assertThat(chunk.metadata().status(), equalTo(StreamMetadata.Status.RUNNING));
            assertThat("waited for the timeout", elapsed, greaterThanOrEqualTo(250L));
            assertThat("and not much longer", elapsed, lessThan(10_000L));
        }
        // rows that arrive afterwards are there for the next GET
        writer.append(4);
        try (StreamPages chunk = safeGet(wait("s", 0, TimeValue.ZERO))) {
            assertThat(chunk.rows(), equalTo(4L));
        }
    }

    public void testRowsAlreadyStoredAreReturnedAtOnceWhateverTheTimeout() {
        createStream("s").append(5);
        long start = System.nanoTime();
        try (StreamPages chunk = safeGet(wait("s", 0, TimeValue.timeValueSeconds(60)))) {
            assertThat("a running stream is not waited for", elapsedMillis(start), lessThan(20_000L));
            assertThat(chunk.rows(), equalTo(5L));
            assertThat(chunk.isTerminal(), equalTo(false));
        }
    }

    public void testFewerRowsThanRequestedDoesNotWaitToFillTheRequest() {
        createStream("s").append(3);
        long start = System.nanoTime();
        try (StreamPages chunk = safeGet(wait("s", 0, TimeValue.timeValueSeconds(60)))) {
            // asked for CHUNK rows, only 3 exist: they come back at once
            assertThat(elapsedMillis(start), lessThan(20_000L));
            assertThat(chunk.rows(), equalTo(3L));
        }
    }

    public void testStreamKeepsAddingPagesButTheWaitDoesNotWaitForCompletion() {
        Writer writer = createStream("s").append(2);
        later(50, () -> writer.append(2));
        long start = System.nanoTime();
        try (StreamPages chunk = safeGet(wait("s", 0, TimeValue.timeValueSeconds(60)))) {
            assertThat(elapsedMillis(start), lessThan(20_000L));
            assertThat(chunk.metadata().status(), equalTo(StreamMetadata.Status.RUNNING));
        }
    }

    public void testNoWaitWhenTheTimeoutIsUnsetZeroOrNegative() {
        createStream("s");
        for (TimeValue timeout : new TimeValue[] { TimeValue.MINUS_ONE, TimeValue.ZERO }) {
            long start = System.nanoTime();
            try (StreamPages chunk = safeGet(wait("s", 0, timeout))) {
                assertThat(chunk.pages(), equalTo(List.of()));
                assertThat("no wait for " + timeout, elapsedMillis(start), lessThan(5_000L));
            }
        }
    }

    public void testCompletionEndsTheWait() {
        Writer writer = createStream("s");
        later(100, writer::complete);
        long start = System.nanoTime();
        try (StreamPages chunk = safeGet(wait("s", 0, TimeValue.timeValueSeconds(30)))) {
            assertThat(elapsedMillis(start), lessThan(20_000L));
            assertThat(chunk.metadata().status(), equalTo(StreamMetadata.Status.COMPLETE));
            assertThat(chunk.pages(), equalTo(List.of()));
        }
    }

    public void testFailureEndsTheWait() {
        Writer writer = createStream("s");
        later(100, () -> writer.fail(new IllegalStateException("boom")));
        long start = System.nanoTime();
        try (StreamPages chunk = safeGet(wait("s", 0, TimeValue.timeValueSeconds(30)))) {
            assertThat(elapsedMillis(start), lessThan(20_000L));
            assertThat(chunk.metadata().status(), equalTo(StreamMetadata.Status.FAILED));
        }
    }

    public void testCancelledStatusEndsTheWait() {
        createStream("s");
        AtomicInteger calls = new AtomicInteger();
        StreamStorage cancelling = new DelegatingStreamStorage(store) {
            @Override
            public void getMetadata(String streamId, ActionListener<StreamMetadata> listener) {
                boolean cancel = calls.incrementAndGet() > 2;
                super.getMetadata(
                    streamId,
                    listener.map(
                        m -> cancel
                            ? new StreamMetadata(
                                m.streamId(),
                                m.taskId(),
                                m.expiration(),
                                m.columns(),
                                m.headers(),
                                m.warnings(),
                                StreamMetadata.Status.CANCELED,
                                null
                            )
                            : m
                    )
                );
            }
        };
        try (StreamPages chunk = safeGet(wait(newTask(), cancelling, "s", 0, TimeValue.timeValueSeconds(30)))) {
            assertThat(chunk.metadata().status(), equalTo(StreamMetadata.Status.CANCELED));
        }
    }

    public void testAlreadyTerminalDoesNotWait() {
        createStream("s").append(2).complete();
        long start = System.nanoTime();
        try (StreamPages chunk = safeGet(wait("s", 2, TimeValue.timeValueSeconds(60)))) {
            assertThat(chunk.pages(), equalTo(List.of()));
            assertThat(elapsedMillis(start), lessThan(20_000L));
        }
    }

    public void testStreamDeletedDuringTheWaitIsNotFound() {
        createStream("s");
        later(100, () -> awaitVoid(l -> store.delete("s", l)));
        Exception e = safeAwaitFailure(
            StreamPages.class,
            l -> StreamTaskManagementService.addCompletionListener(
                threadPool,
                newTask(),
                store,
                "s",
                0,
                CHUNK,
                TimeValue.timeValueSeconds(30),
                l
            )
        );
        assertThat(e, instanceOf(ResourceNotFoundException.class));
    }

    public void testTtlExpiringDuringTheWaitIsNotFound() {
        createStream("s", java.util.Map.of(), System.currentTimeMillis() + 200);
        Exception e = safeAwaitFailure(
            StreamPages.class,
            l -> StreamTaskManagementService.addCompletionListener(
                threadPool,
                newTask(),
                store,
                "s",
                0,
                CHUNK,
                TimeValue.timeValueSeconds(30),
                l
            )
        );
        assertThat(e, instanceOf(ResourceNotFoundException.class));
    }

    public void testNotFoundFromTheReadDuringTheWait() {
        createStream("s");
        AtomicInteger calls = new AtomicInteger();
        StreamStorage failing = new DelegatingStreamStorage(store) {
            @Override
            public void getPages(String streamId, long offset, long rows, ActionListener<List<Page>> listener) {
                if (calls.incrementAndGet() > 2) {
                    listener.onFailure(new ResourceNotFoundException(streamId));
                } else {
                    super.getPages(streamId, offset, rows, listener);
                }
            }
        };
        Exception e = safeAwaitFailure(
            StreamPages.class,
            l -> StreamTaskManagementService.addCompletionListener(
                threadPool,
                newTask(),
                failing,
                "s",
                0,
                CHUNK,
                TimeValue.timeValueSeconds(30),
                l
            )
        );
        assertThat(e, instanceOf(ResourceNotFoundException.class));
    }

    public void testCancellingTheTaskStopsTheWait() {
        createStream("s");
        CancellableTask task = newTask();
        PlainActionFuture<StreamPages> future = wait(task, store, "s", 0, TimeValue.timeValueSeconds(60));
        later(150, () -> org.elasticsearch.tasks.TaskCancelHelper.cancel(task, "test"));
        long start = System.nanoTime();
        Exception e = expectThrows(Exception.class, () -> future.actionGet(30, TimeUnit.SECONDS));
        assertThat(ExceptionsHelper.unwrap(e, TaskCancelledException.class), instanceOf(TaskCancelledException.class));
        assertThat("stops long before the timeout", elapsedMillis(start), lessThan(20_000L));
    }

    public void testEveryAttemptReadsTheMetadataBeforeThePages() {
        Writer writer = createStream("s");
        RecordingStore recording = new RecordingStore();
        later(400, () -> writer.append(1));
        try (StreamPages chunk = safeGet(wait(newTask(), recording, "s", 0, TimeValue.timeValueSeconds(30)))) {
            assertThat(chunk.rows(), equalTo(1L));
        }
        List<String> calls = List.copyOf(recording.calls);
        assertThat("it polled more than once", calls.size(), greaterThanOrEqualTo(4));
        for (int i = 0; i < calls.size(); i += 2) {
            assertThat(calls.get(i), equalTo("metadata"));
            assertThat(calls.get(i + 1), equalTo("pages"));
        }
    }

    public void testAttemptsWithoutRowsHoldNoPages() {
        createStream("s");
        try (StreamPages chunk = safeGet(wait("s", 0, TimeValue.timeValueMillis(300)))) {
            assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        }
    }

    public void testTheWaitStillFiresWhenTheSearchPoolIsBusy() throws Exception {
        // the polling must not run on the SEARCH pool, so a pool that is fully busy cannot delay the timeout
        int threads = threadPool.info(ThreadPool.Names.SEARCH).getMax();
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch occupied = new CountDownLatch(threads);
        for (int i = 0; i < threads; i++) {
            threadPool.executor(ThreadPool.Names.SEARCH).execute(() -> {
                occupied.countDown();
                safeAwait(release);
            });
        }
        try {
            safeAwait(occupied);
            createStream("s");
            PlainActionFuture<StreamPages> future = new PlainActionFuture<>();
            StreamTaskManagementService.addCompletionListener(
                threadPool,
                newTask(),
                store,
                "s",
                0,
                CHUNK,
                TimeValue.timeValueMillis(200),
                future
            );
            try (StreamPages chunk = future.actionGet(20, TimeUnit.SECONDS)) {
                assertThat(chunk.pages(), equalTo(List.of()));
            }
        } finally {
            release.countDown();
        }
    }

    public void testManyWaitersDoNotBlockThreads() {
        createStream("s");
        int waiters = 50;
        List<PlainActionFuture<StreamPages>> futures = new java.util.ArrayList<>();
        for (int i = 0; i < waiters; i++) {
            futures.add(wait("s", 0, TimeValue.timeValueSeconds(30)));
        }
        // other work still runs on the same pool while the waiters wait
        PlainActionFuture<Void> other = new PlainActionFuture<>();
        threadPool.generic().execute(() -> other.onResponse(null));
        safeGet(other);
        awaitVoid(l -> store.appendPage("s", 0, page(0, 1), l));
        for (PlainActionFuture<StreamPages> future : futures) {
            try (StreamPages chunk = safeGet(future)) {
                assertThat(chunk.rows(), equalTo(1L));
            }
        }
    }
}
