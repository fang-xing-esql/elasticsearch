/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;

/**
 * Reads from a {@link StreamStorage} and waits for rows, the counterpart of {@code AsyncTaskManagementService}.
 * <p>
 * The wait follows the rules of {@code wait_for_completion_timeout} of the async API: a timeout that is not set, zero or negative does
 * not wait, a positive timeout is a total deadline, and reaching it is not an error. One thing differs on purpose: a stream may stay
 * running for a long time while it keeps adding pages, so the wait does not wait for completion. It ends as soon as a read returns rows or
 * the stream is no longer running.
 * <p>
 * There is no task to listen to, so waiting polls the storage on the scheduler and never blocks a thread.
 */
final class StreamTaskManagementService {

    static final long INITIAL_POLL_INTERVAL_MILLIS = 50;
    static final long MAX_POLL_INTERVAL_MILLIS = 250;

    private StreamTaskManagementService() {}

    /**
     * Reads the metadata and then the pages of {@code [offset, offset + rows)}, once. The metadata is read first, so a terminal status
     * guarantees that an empty read is the end of the result.
     * <p>
     * Fails with {@link ResourceNotFoundException} if the stream is unknown or expired.
     */
    static void getCurrentResult(StreamStorage store, String id, long offset, long rows, ActionListener<StreamPages> listener) {
        store.getMetadata(id, listener.delegateFailureAndWrap((l, metadata) -> {
            if (metadata == null || metadata.ttl().toEpochMilli() < System.currentTimeMillis()) {
                throw new ResourceNotFoundException(id);
            }
            store.getPages(id, offset, rows, l.delegateFailureAndWrap((l2, pages) -> l2.onResponse(new StreamPages(metadata, pages))));
        }));
    }

    /**
     * Like {@link #getCurrentResult}, but when {@code timeout} is positive and a read finds no rows in a running stream, reads again
     * until rows are found, the stream stops running or the timeout elapses. The timeout is a total deadline that never resets.
     */
    static void addCompletionListener(
        ThreadPool threadPool,
        CancellableTask task,
        StreamStorage store,
        String id,
        long offset,
        long rows,
        TimeValue timeout,
        ActionListener<StreamPages> listener
    ) {
        if (timeout.getMillis() <= 0) {
            getCurrentResult(store, id, offset, rows, listener);
            return;
        }
        long deadlineNanos = System.nanoTime() + timeout.nanos();
        attempt(threadPool, task, store, id, offset, rows, deadlineNanos, INITIAL_POLL_INTERVAL_MILLIS, listener);
    }

    private static void attempt(
        ThreadPool threadPool,
        CancellableTask task,
        StreamStorage store,
        String id,
        long offset,
        long rows,
        long deadlineNanos,
        long pollIntervalMillis,
        ActionListener<StreamPages> listener
    ) {
        if (task != null && task.notifyIfCancelled(listener)) {
            return;
        }
        getCurrentResult(store, id, offset, rows, listener.delegateFailureAndWrap((l, chunk) -> {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (chunk.pages().isEmpty() == false || chunk.isTerminal() || remainingNanos <= 0) {
                l.onResponse(chunk);
                return;
            }
            // Nothing to return yet and the stream is still running: poll again, never later than the deadline.
            long delayMillis = Math.min(pollIntervalMillis, Math.max(1, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
            long nextInterval = Math.min(pollIntervalMillis * 2, MAX_POLL_INTERVAL_MILLIS);
            threadPool.schedule(
                ActionRunnable.wrap(l, ignored -> attempt(threadPool, task, store, id, offset, rows, deadlineNanos, nextInterval, l)),
                TimeValue.timeValueMillis(delayMillis),
                threadPool.generic()
            );
        }));
    }
}
