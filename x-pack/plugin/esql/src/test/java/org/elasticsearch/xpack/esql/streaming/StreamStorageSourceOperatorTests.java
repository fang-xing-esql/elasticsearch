/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * {@link StreamStorageSourceOperator} on its own, called the way a driver calls an operator: {@code getOutput} while it is not
 * finished, and a wait on {@code isBlocked} when it has no page. What a client sees when the operator runs in a real driver behind a
 * stream GET is tested by {@link StreamPageProducerTests}. The breaker check after each test fails if a page is not released.
 */
public class StreamStorageSourceOperatorTests extends AbstractStreamTestCase {

    private static final long NO_LIMIT = GetStreamResultRequest.NO_LIMIT;

    private Writer stream(int... pageRows) {
        Writer writer = createStream("s");
        for (int rows : pageRows) {
            writer.append(rows);
        }
        return writer;
    }

    /** Makes the first read as the GET does before the response starts, and creates the operator with it. */
    private StreamStorageSourceOperator operator(StreamStorage storage, long offset, long maxRows, int batchPages) {
        StreamPages first = safeAwait(
            l -> StreamTaskManagementService.getCurrentResult(
                storage,
                "s",
                StreamTaskManagementService.pageBatchReader(offset, offset, batchPages, 0),
                l
            )
        );
        DriverContext driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        return new StreamStorageSourceOperator(driverContext, storage, "s", offset, first, maxRows, batchPages, 0);
    }

    /** Runs the operator to its end and returns the values it emitted, one list for each page. */
    private static List<List<Long>> drain(StreamStorageSourceOperator operator) {
        List<List<Long>> pages = new ArrayList<>();
        while (operator.isFinished() == false) {
            Page page = operator.getOutput();
            if (page == null) {
                safeAwait(operator.isBlocked().listener());
                continue;
            }
            try {
                LongBlock block = page.getBlock(0);
                List<Long> values = new ArrayList<>();
                for (int i = 0; i < page.getPositionCount(); i++) {
                    values.add(block.getLong(i));
                }
                pages.add(values);
            } finally {
                page.releaseBlocks();
            }
        }
        return pages;
    }

    private static List<Long> flat(List<List<Long>> pages) {
        return pages.stream().flatMap(List::stream).toList();
    }

    public void testEmitsTheStoredPagesInOrder() {
        stream(4, 3, 5).complete();
        RecordingStore recording = new RecordingStore();
        StreamStorageSourceOperator operator = operator(recording, 0, NO_LIMIT, 1);
        try {
            assertThat(drain(operator), equalTo(List.of(range(0, 4), range(4, 7), range(7, 12))));
            assertThat("one batch of one page for each page, and no read that comes back empty", recording.pageReads(), equalTo(3));
            assertThat(operator.metadata().status(), equalTo(StreamMetadata.Status.COMPLETE));
        } finally {
            operator.close();
        }
    }

    public void testOffsetInsideAPage() {
        stream(4, 3, 5).complete();
        StreamStorageSourceOperator operator = operator(store, 5, NO_LIMIT, between(1, 4));
        try {
            assertThat("the page that contains the offset is trimmed", drain(operator), equalTo(List.of(range(5, 7), range(7, 12))));
        } finally {
            operator.close();
        }
    }

    public void testMaxRowsTrimsTheLastPageAndStopsTheReads() {
        stream(4, 3, 5, 2).complete();
        RecordingStore recording = new RecordingStore();
        StreamStorageSourceOperator operator = operator(recording, 1, 5, 1);
        try {
            assertThat(drain(operator), equalTo(List.of(range(1, 4), range(4, 6))));
            assertThat("the two pages that hold the five rows", recording.pageReads(), equalTo(2));
        } finally {
            operator.close();
        }
        // a limit at a page boundary needs no trimming and reads nothing after it
        recording = new RecordingStore();
        operator = operator(recording, 0, 4, 1);
        try {
            assertThat(drain(operator), equalTo(List.of(range(0, 4))));
            assertThat(recording.pageReads(), equalTo(1));
        } finally {
            operator.close();
        }
    }

    public void testReadsOnlyWhenAskedForAPage() {
        stream(2, 2, 2).complete();
        RecordingStore recording = new RecordingStore();
        StreamStorageSourceOperator operator = operator(recording, 0, NO_LIMIT, 1);
        try {
            assertThat(recording.pageReads(), equalTo(1));
            assertThat("it holds the first page, so it is not blocked", operator.isBlocked().listener().isDone(), equalTo(true));
            assertThat(operator.isFinished(), equalTo(false));
            assertThat("asking whether it is blocked or finished reads nothing", recording.pageReads(), equalTo(1));
            operator.getOutput().releaseBlocks();
            assertThat("emitting the page it holds reads nothing", recording.pageReads(), equalTo(1));
            assertThat("with no page left, being asked for one starts the read", operator.getOutput(), nullValue());
            assertThat(recording.pageReads(), equalTo(2));
        } finally {
            operator.close();
        }
    }

    public void testRunningStreamEndsWithTheFirstEmptyBatch() {
        Writer writer = stream(3, 3);
        StreamStorageSourceOperator operator = operator(store, 0, NO_LIMIT, 1);
        try {
            assertThat(flat(drain(operator)), equalTo(range(0, 6)));
            assertThat(operator.metadata().status(), equalTo(StreamMetadata.Status.RUNNING));
        } finally {
            operator.close();
        }
        // rows stored and a completion that happen between two batches are seen by the later batch
        operator = operator(store, 0, NO_LIMIT, 1);
        try {
            operator.getOutput().releaseBlocks();
            writer.append(2).complete();
            assertThat(flat(drain(operator)), equalTo(range(3, 8)));
            assertThat(operator.metadata().status(), equalTo(StreamMetadata.Status.COMPLETE));
        } finally {
            operator.close();
        }
    }

    public void testBlockedWhileAReadIsRunning() {
        stream(2, 2).complete();
        AtomicReference<Runnable> pendingRead = new AtomicReference<>();
        StreamStorage slow = new DelegatingStreamStorage(store) {
            private boolean first = true;

            @Override
            public void getPageBatch(String streamId, long fromOffset, int maxPages, long maxBytes, ActionListener<PageBatch> listener) {
                if (first) {
                    first = false;
                    super.getPageBatch(streamId, fromOffset, maxPages, maxBytes, listener);
                } else {
                    pendingRead.set(() -> super.getPageBatch(streamId, fromOffset, maxPages, maxBytes, listener));
                }
            }
        };
        StreamStorageSourceOperator operator = operator(slow, 0, NO_LIMIT, 1);
        try {
            operator.getOutput().releaseBlocks();
            assertThat(operator.getOutput(), nullValue());
            assertThat("blocked until the storage answers", operator.isBlocked().listener().isDone(), equalTo(false));
            assertThat(operator.isFinished(), equalTo(false));
            assertThat("a second read is not started while one is running", operator.getOutput(), nullValue());
            pendingRead.get().run();
            assertThat(operator.isBlocked().listener().isDone(), equalTo(true));
            assertThat(flat(drain(operator)), equalTo(range(2, 4)));
        } finally {
            operator.close();
        }
    }

    public void testClosedWhileAReadIsRunningReleasesItsPages() {
        stream(2, 2).complete();
        AtomicReference<Runnable> pendingRead = new AtomicReference<>();
        StreamStorage slow = new DelegatingStreamStorage(store) {
            private boolean first = true;

            @Override
            public void getPageBatch(String streamId, long fromOffset, int maxPages, long maxBytes, ActionListener<PageBatch> listener) {
                if (first) {
                    first = false;
                    super.getPageBatch(streamId, fromOffset, maxPages, maxBytes, listener);
                } else {
                    pendingRead.set(() -> super.getPageBatch(streamId, fromOffset, maxPages, maxBytes, listener));
                }
            }
        };
        StreamStorageSourceOperator operator = operator(slow, 0, NO_LIMIT, 1);
        operator.getOutput().releaseBlocks();
        assertThat(operator.getOutput(), nullValue());
        operator.close();
        // the storage answers after the operator is gone: the breaker check fails if these pages are kept
        pendingRead.get().run();
    }

    public void testFailedReadIsThrownToTheDriver() {
        stream(2, 2, 2).complete();
        StreamStorage failing = new DelegatingStreamStorage(store) {
            private int reads;

            @Override
            public void getPageBatch(String streamId, long fromOffset, int maxPages, long maxBytes, ActionListener<PageBatch> listener) {
                if (++reads == 2) {
                    listener.onFailure(new IllegalStateException("the storage is broken"));
                } else {
                    super.getPageBatch(streamId, fromOffset, maxPages, maxBytes, listener);
                }
            }
        };
        StreamStorageSourceOperator operator = operator(failing, 0, NO_LIMIT, 1);
        try {
            operator.getOutput().releaseBlocks();
            assertThat(operator.getOutput(), nullValue());
            assertThat("not finished, so that the driver asks again and gets the failure", operator.isFinished(), equalTo(false));
            Exception e = expectThrows(IllegalStateException.class, operator::getOutput);
            assertThat(e.getMessage(), equalTo("the storage is broken"));
            assertThat(operator.isFinished(), equalTo(true));
        } finally {
            operator.close();
        }
    }

    public void testStreamDeletedBetweenTwoBatches() {
        stream(2, 2).complete();
        StreamStorageSourceOperator operator = operator(store, 0, NO_LIMIT, 1);
        try {
            operator.getOutput().releaseBlocks();
            awaitVoid(l -> store.delete("s", l));
            assertThat(operator.getOutput(), nullValue());
            assertThat(expectThrows(RuntimeException.class, operator::getOutput), instanceOf(ResourceNotFoundException.class));
        } finally {
            operator.close();
        }
    }

    public void testFinishStopsTheOperator() {
        stream(2, 2).complete();
        StreamStorageSourceOperator operator = operator(store, 0, NO_LIMIT, 1);
        operator.finish();
        assertThat(operator.isFinished(), equalTo(true));
        operator.close();
    }
}
