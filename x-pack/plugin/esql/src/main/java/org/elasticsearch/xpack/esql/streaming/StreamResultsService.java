/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * Reads the stored result of a stream, the counterpart of {@code AsyncResultsService}.
 * <p>
 * The flow is: look up the metadata, check that the stream exists, has not expired and belongs to the caller (all three give the same
 * 404), then read the pages once, waiting for rows if the request asked for it, and return them. It only uses the {@link StreamStorage}.
 * There is no task or node memory to look at.
 * <p>
 * The first read is made here, so that a failure before the first row is a normal HTTP error. What follows depends on the
 * {@link GetStreamResultRequest.ReadMode} of the request:
 * <ul>
 *   <li>{@code PAGES}: the first read is a single {@code getPages} call, for {@code max_rows} rows or, when it is not set, for every
 *       row from the offset. The pages are held until they are written, so the memory a GET uses is the size of what it returns.</li>
 *   <li>{@code BATCH}: the first read is one {@code getPageBatch} call, and the next batches are read while the response is written,
 *       only when the client is ready for more. The memory a GET uses is one batch.</li>
 * </ul>
 * In both modes the pages reach the response through a {@link StreamPageProducer} and its publisher, the way the pages of a running
 * query reach the response of the streaming POST.
 */
public class StreamResultsService {

    private final StreamStorage storage;
    private final StreamSearchSecurity security;
    private final ThreadPool threadPool;
    private final StreamStorageDriver.Settings driverSettings;
    private final int batchPages;
    private final long batchBytes;

    public StreamResultsService(
        StreamStorage storage,
        StreamSearchSecurity security,
        ThreadPool threadPool,
        StreamStorageDriver.Settings driverSettings
    ) {
        this(
            storage,
            security,
            threadPool,
            driverSettings,
            StreamStoragePageProducer.DEFAULT_BATCH_PAGES,
            StreamStoragePageProducer.DEFAULT_BATCH_BYTES
        );
    }

    /**
     * @param driverSettings what the {@code DRIVER} read mode needs to run a driver
     * @param batchPages the number of pages of one batch in the {@code BATCH} read mode
     * @param batchBytes the bound on the stored bytes of one batch in the {@code BATCH} read mode
     */
    public StreamResultsService(
        StreamStorage storage,
        StreamSearchSecurity security,
        ThreadPool threadPool,
        StreamStorageDriver.Settings driverSettings,
        int batchPages,
        long batchBytes
    ) {
        this.storage = storage;
        this.security = security;
        this.threadPool = threadPool;
        this.driverSettings = driverSettings;
        this.batchPages = batchPages;
        this.batchBytes = batchBytes;
    }

    public void retrieveResult(CancellableTask task, GetStreamResultRequest request, ActionListener<GetStreamResultResponse> listener) {
        StreamStorage store = storage;
        String id = request.id();
        store.getMetadata(id, listener.delegateFailureAndWrap((l, metadata) -> {
            // Security and expiry are checked once, up front, before any wait or page read.
            // TODO: this compares the caller with the security headers stored in the metadata, but nothing stores them yet: the stream
            // POST does not call StreamStorage#create. When it does, it must pass the security headers of the request that started the
            // query. Empty headers mean "created with security off" and let every caller read the stream. See StreamSearchSecurity.
            StreamMetadata checked = security.getStreamAndCheckAuthentication(id, metadata, System.currentTimeMillis());
            if (request.statusOnly()) {
                respond(task, store, request, new StreamPages(checked, List.of(), request.offset()), l);
                return;
            }
            StreamTaskManagementService.PageReader reader = switch (request.readMode()) {
                case PAGES -> StreamTaskManagementService.rowRangeReader(request.offset(), rowsToRead(request));
                case BATCH, DRIVER -> StreamTaskManagementService.pageBatchReader(
                    request.offset(),
                    request.offset(),
                    batchPages,
                    batchBytes
                );
            };
            StreamTaskManagementService.addCompletionListener(
                threadPool,
                task,
                store,
                id,
                reader,
                request.waitTimeout(),
                l.delegateFailureAndWrap((l2, pages) -> respond(task, store, request, pages, l2))
            );
        }));
    }

    /**
     * The number of rows to ask the storage for: {@code max_rows} when it is set, otherwise every row from the offset. It is clamped so
     * that {@code offset + rows} cannot overflow inside the storage.
     */
    static long rowsToRead(GetStreamResultRequest request) {
        long rest = Long.MAX_VALUE - request.offset();
        return request.hasRowLimit() ? Math.min(request.maxRows(), rest) : rest;
    }

    private void respond(
        CancellableTask task,
        StreamStorage store,
        GetStreamResultRequest request,
        StreamPages pages,
        ActionListener<GetStreamResultResponse> listener
    ) {
        Exception failure = validate(request, pages);
        if (failure != null) {
            pages.close();
            listener.onFailure(failure);
            return;
        }
        // TODO: decide which executor reads the next batches when the real storage exists. It must not be an HTTP thread.
        Executor executor = threadPool.generic();
        StreamMetadata metadata = pages.metadata();
        StreamResultPages result;
        if (request.statusOnly() || request.readMode() == GetStreamResultRequest.ReadMode.PAGES) {
            result = started(new StreamStorageSingleReadProducer(executor, pages, request.maxRows()));
        } else if (request.readMode() == GetStreamResultRequest.ReadMode.BATCH) {
            result = started(
                new StreamStoragePageProducer(
                    executor,
                    store,
                    request.id(),
                    request.offset(),
                    pages,
                    request.maxRows(),
                    batchPages,
                    batchBytes
                )
            );
        } else {
            assert request.readMode() == GetStreamResultRequest.ReadMode.DRIVER : request.readMode();
            result = StreamStorageDriver.start(
                driverSettings,
                threadPool.getThreadContext(),
                task,
                store,
                request,
                pages,
                batchPages,
                batchBytes
            );
        }
        listener.onResponse(new GetStreamResultResponse(request, metadata, result));
    }

    private static StreamPageProducer started(StreamPageProducer producer) {
        producer.start();
        return producer;
    }

    /** Returns the exception that makes this read an HTTP error instead of a response, or {@code null}. */
    private static Exception validate(GetStreamResultRequest request, StreamPages pages) {
        StreamMetadata metadata = pages.metadata();
        if (metadata.status() == StreamMetadata.Status.COMPLETE && metadata.completionInfo() != null) {
            Long totalRows = metadata.completionInfo().rows();
            if (totalRows != null && request.offset() > totalRows) {
                return new IllegalArgumentException(
                    "offset [" + request.offset() + "] is beyond the end of the result [" + totalRows + "]"
                );
            }
        }
        // A failure before the first row is a normal HTTP error. After rows were written the status is already 200, and the error
        // goes in the last line instead.
        if (request.statusOnly() == false
            && metadata.status() == StreamMetadata.Status.FAILED
            && pages.pages().isEmpty()
            && metadata.completionInfo() != null
            && metadata.completionInfo().failure() != null) {
            return metadata.completionInfo().failure().toException();
        }
        return null;
    }
}
