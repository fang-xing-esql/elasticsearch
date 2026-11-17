/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.action.ActionListener;

import java.util.concurrent.Executor;

/**
 * The producer of the {@link GetStreamResultRequest.ReadMode#BATCH} mode: it reads a batch of whole pages with
 * {@link StreamStorage#getPageBatch} each time the client is ready for a page and none is left, starting where the previous batch
 * ended. So it holds one batch at most, whatever the size of the result, and a response that stops early reads nothing more.
 * <p>
 * Every read checks the metadata first, so a stream that is deleted or expires while it is read ends the response with a not found
 * error in the last line.
 */
public final class StreamStoragePageProducer extends StreamPageProducer {

    /** The default number of pages of one batch. */
    public static final int DEFAULT_BATCH_PAGES = 16;
    /** The default bound on the stored bytes of one batch. A batch ends with the page that reaches it. */
    public static final long DEFAULT_BATCH_BYTES = 4 * 1024 * 1024;

    private final StreamStorage store;
    private final String id;
    private final long requestOffset;
    private final int batchPages;
    private final long batchBytes;

    /**
     * @param requestOffset the offset of the request. No row before it is returned.
     * @param first         the first batch, read before the response started
     */
    public StreamStoragePageProducer(
        Executor executor,
        StreamStorage store,
        String id,
        long requestOffset,
        StreamPages first,
        long maxRows,
        int batchPages,
        long batchBytes
    ) {
        super(executor, first, maxRows);
        this.store = store;
        this.id = id;
        this.requestOffset = requestOffset;
        this.batchPages = batchPages;
        this.batchBytes = batchBytes;
    }

    @Override
    protected boolean mayHaveMore(StreamPages last) {
        return storageMayHaveMore(last);
    }

    /** Whether a read after {@code last} can return pages. The same rule ends the reads of {@link StreamStorageSourceOperator}. */
    static boolean storageMayHaveMore(StreamPages last) {
        // A complete stream has a known end, which saves the read that would come back empty.
        StreamMetadata metadata = last.metadata();
        if (metadata.status() == StreamMetadata.Status.COMPLETE && metadata.completionInfo() != null) {
            return last.nextOffset() < metadata.completionInfo().rows();
        }
        return true;
    }

    @Override
    protected void readNext(long fromOffset, ActionListener<StreamPages> listener) {
        StreamTaskManagementService.getCurrentResult(
            store,
            id,
            StreamTaskManagementService.pageBatchReader(requestOffset, fromOffset, batchPages, batchBytes),
            listener
        );
    }
}
