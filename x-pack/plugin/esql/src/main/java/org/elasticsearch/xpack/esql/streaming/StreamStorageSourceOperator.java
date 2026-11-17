/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;

import java.util.ArrayDeque;

/**
 * A source operator that emits the stored pages of a stream. It reads the storage for a stream GET the way
 * {@code LuceneSourceOperator} reads a shard for a query, so that the GET can run as a driver that ends in the same sink as the
 * streaming POST.
 * <p>
 * It reads a batch of whole pages with {@link StreamStorage#getPageBatch} when the driver asks for a page and it has none left. The
 * driver only asks when the sink needs input, which is when the client is ready for a page, so nothing is read ahead of the client.
 * While a read is running the operator is blocked, and the driver holds no thread.
 * <p>
 * The driver calls the operator from one thread at a time. A read completes on a thread of the storage, which only hands the result
 * over under the lock. The driver thread takes it from there.
 */
public final class StreamStorageSourceOperator extends SourceOperator {

    private final DriverContext driverContext;
    private final StreamStorage store;
    private final String id;
    private final long requestOffset;
    private final int batchPages;
    private final long batchBytes;

    /** Pages ready to be emitted. Only used by the driver thread. */
    private final ArrayDeque<Page> ready = new ArrayDeque<>();
    /** Rows that may still be emitted before {@code max_rows} is reached. Only used by the driver thread. */
    private long rowsLeft;
    /** Where the next read from the storage starts. Only used by the driver thread. */
    private long nextOffset;
    /** Whether the storage may have more pages. Only used by the driver thread. */
    private boolean mayHaveMore;
    private boolean finished;
    private int batchesRead;

    /** The read that is running, or {@code null}. Guarded by {@code this}. */
    private SubscribableListener<Void> readInFlight;
    /** The result of a read that the driver thread has not taken yet. Guarded by {@code this}. */
    private StreamPages arrived;
    /** The failure of a read that the driver thread has not thrown yet. Guarded by {@code this}. */
    private Exception failure;
    /** Guarded by {@code this}. */
    private boolean closed;

    private volatile StreamMetadata metadata;

    /**
     * @param requestOffset the offset of the request. No row before it is emitted.
     * @param first         the result of the first read, made before the response started. The operator takes its pages.
     * @param maxRows       the largest number of rows to emit, or {@link GetStreamResultRequest#NO_LIMIT}
     * @param batchPages    the number of pages of one batch
     * @param batchBytes    the bound on the stored bytes of one batch
     */
    public StreamStorageSourceOperator(
        DriverContext driverContext,
        StreamStorage store,
        String id,
        long requestOffset,
        StreamPages first,
        long maxRows,
        int batchPages,
        long batchBytes
    ) {
        this.driverContext = driverContext;
        this.store = store;
        this.id = id;
        this.requestOffset = requestOffset;
        this.batchPages = batchPages;
        this.batchBytes = batchBytes;
        this.rowsLeft = maxRows == GetStreamResultRequest.NO_LIMIT ? Long.MAX_VALUE : maxRows;
        accept(first);
    }

    /** The metadata of the latest read from the storage. The last line of the response is written from it. */
    public StreamMetadata metadata() {
        return metadata;
    }

    /** Takes the pages of a read, trimmed to the rows that may still be emitted. */
    private void accept(StreamPages read) {
        metadata = read.metadata();
        nextOffset = read.nextOffset();
        for (Page page : read.pages()) {
            if (rowsLeft == 0) {
                page.releaseBlocks();
            } else if (page.getPositionCount() > rowsLeft) {
                try {
                    ready.add(page.slice(0, Math.toIntExact(rowsLeft)));
                } finally {
                    page.releaseBlocks();
                }
                rowsLeft = 0;
            } else {
                ready.add(page);
                rowsLeft -= page.getPositionCount();
            }
        }
        // An empty read is the end of what is stored for now. A GET does not wait for more rows once it has started to answer.
        mayHaveMore = rowsLeft > 0 && read.pages().isEmpty() == false && StreamStoragePageProducer.storageMayHaveMore(read);
    }

    /** Moves the result of a finished read to the driver thread, or throws its failure. */
    private void takeArrived() {
        StreamPages read;
        Exception e;
        synchronized (this) {
            read = arrived;
            arrived = null;
            e = failure;
            failure = null;
        }
        if (e != null) {
            mayHaveMore = false;
            throw e instanceof RuntimeException re ? re : new RuntimeException(e);
        }
        if (read != null) {
            accept(read);
        }
    }

    @Override
    public Page getOutput() {
        takeArrived();
        Page page = ready.poll();
        if (page != null) {
            return page;
        }
        if (mayHaveMore) {
            startRead();
        }
        return null;
    }

    private void startRead() {
        SubscribableListener<Void> blocked = new SubscribableListener<>();
        synchronized (this) {
            if (readInFlight != null || closed) {
                return;
            }
            readInFlight = blocked;
        }
        batchesRead++;
        // The driver waits for the read before it reports that it is done, so that the pages of a late read are released first.
        driverContext.addAsyncAction();
        StreamTaskManagementService.getCurrentResult(
            store,
            id,
            StreamTaskManagementService.pageBatchReader(requestOffset, nextOffset, batchPages, batchBytes),
            ActionListener.runAfter(ActionListener.wrap(read -> onRead(read, null), e -> onRead(null, e)), () -> {
                driverContext.removeAsyncAction();
                blocked.onResponse(null);
            })
        );
    }

    private void onRead(StreamPages read, Exception e) {
        boolean release;
        synchronized (this) {
            readInFlight = null;
            release = closed;
            if (release == false) {
                arrived = read;
                failure = e;
            }
        }
        if (release && read != null) {
            read.close();
        }
    }

    @Override
    public IsBlockedResult isBlocked() {
        SubscribableListener<Void> blocked;
        synchronized (this) {
            blocked = readInFlight;
        }
        return blocked == null ? Operator.NOT_BLOCKED : new IsBlockedResult(blocked, "stream storage read");
    }

    @Override
    public boolean isFinished() {
        if (finished) {
            return true;
        }
        synchronized (this) {
            if (readInFlight != null || arrived != null || failure != null) {
                return false;
            }
        }
        return ready.isEmpty() && mayHaveMore == false;
    }

    @Override
    public void finish() {
        finished = true;
    }

    @Override
    public void close() {
        StreamPages read;
        synchronized (this) {
            closed = true;
            read = arrived;
            arrived = null;
        }
        if (read != null) {
            read.close();
        }
        Page page;
        while ((page = ready.poll()) != null) {
            page.releaseBlocks();
        }
    }

    @Override
    public String toString() {
        return "StreamStorageSourceOperator[stream=" + id + ", nextOffset=" + nextOffset + ", batchesRead=" + batchesRead + "]";
    }
}
