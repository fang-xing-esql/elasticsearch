/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Request to read a stored stream result: the rows from {@link #offset()} on, limited by {@link #maxRows()} and {@link #maxBytes()} when
 * they are set.
 * <p>
 * The request runs on the node that receives it and is never sent to another node, so it is not serializable. Its task is cancellable so
 * that a client disconnect stops a wait for rows.
 */
public class GetStreamResultRequest extends UntypedActionRequest {

    /** Value of {@link #maxRows()} and {@link #maxBytes()} when the limit was not provided. */
    public static final long NO_LIMIT = -1L;

    private final String id;
    private final long offset;
    private final long maxRows;
    private final long maxBytes;
    private final TimeValue waitTimeout;
    private final ReadMode readMode;

    /**
     * How the GET reads the pages from the storage. Both modes return the same response. They exist side by side so that they can be
     * compared in benchmarks. It is not part of the public API.
     */
    public enum ReadMode {
        /** One {@code getPages} call for every row the request may return. All the pages are held until they are written. */
        PAGES,
        /** A {@code getPageBatch} call for a few whole pages at a time, made only when the client is ready for more. */
        BATCH,
        /**
         * The reads of {@link #BATCH}, made by a source operator in a driver that ends in the sink of the streaming POST.
         */
        DRIVER;

        public static final ReadMode DEFAULT = BATCH;

        public static ReadMode fromString(String value) {
            return switch (value) {
                case "pages" -> PAGES;
                case "batch" -> BATCH;
                case "driver" -> DRIVER;
                default -> throw new IllegalArgumentException("unknown [read_mode] [" + value + "], expected [pages], [batch] or [driver]");
            };
        }
    }

    public GetStreamResultRequest(String id, long offset, long maxRows, long maxBytes, TimeValue waitTimeout) {
        this(id, offset, maxRows, maxBytes, waitTimeout, ReadMode.DEFAULT);
    }

    /**
     * @param id          the stream id
     * @param offset      first row to return, {@code >= 0}
     * @param maxRows     stop after this many rows, {@code >= 0}, or {@link #NO_LIMIT}. {@code 0} returns the status only.
     * @param maxBytes    stop after about this many bytes of rows, {@code >= 0}, or {@link #NO_LIMIT}. {@code 0} returns the status only.
     * @param waitTimeout how long to wait for rows when there are none yet. {@code null}, zero or negative means do not wait,
     *                    as an unset {@code wait_for_completion_timeout} does for the async API.
     * @param readMode    how the pages are read from the storage
     */
    public GetStreamResultRequest(String id, long offset, long maxRows, long maxBytes, TimeValue waitTimeout, ReadMode readMode) {
        this.id = id;
        if (offset < 0) {
            throw new IllegalArgumentException("[offset] must be greater than or equal to 0 but was [" + offset + "]");
        }
        if (maxRows < 0 && maxRows != NO_LIMIT) {
            throw new IllegalArgumentException("[max_rows] must be greater than or equal to 0 but was [" + maxRows + "]");
        }
        if (maxBytes < 0 && maxBytes != NO_LIMIT) {
            throw new IllegalArgumentException("[max_bytes] must be greater than or equal to 0 but was [" + maxBytes + "]");
        }
        this.offset = offset;
        this.maxRows = maxRows;
        this.maxBytes = maxBytes;
        this.waitTimeout = waitTimeout == null ? TimeValue.MINUS_ONE : waitTimeout;
        this.readMode = Objects.requireNonNull(readMode);
    }

    public String id() {
        return id;
    }

    public long offset() {
        return offset;
    }

    public long maxRows() {
        return maxRows;
    }

    public long maxBytes() {
        return maxBytes;
    }

    public TimeValue waitTimeout() {
        return waitTimeout;
    }

    public ReadMode readMode() {
        return readMode;
    }

    public boolean hasRowLimit() {
        return maxRows != NO_LIMIT;
    }

    public boolean hasByteLimit() {
        return maxBytes != NO_LIMIT;
    }

    /** {@code max_rows=0} or {@code max_bytes=0}: only the status is returned, no pages are read and there is no wait. */
    public boolean statusOnly() {
        return maxRows == 0 || maxBytes == 0;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (id == null || id.isEmpty()) {
            ActionRequestValidationException e = new ActionRequestValidationException();
            e.addValidationError("id is missing");
            return e;
        }
        return null;
    }

    @Override
    public Task createTask(TaskId taskId, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(taskId.getId(), type, action, "stream get result", parentTaskId, headers);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetStreamResultRequest that = (GetStreamResultRequest) o;
        return offset == that.offset
            && maxRows == that.maxRows
            && maxBytes == that.maxBytes
            && Objects.equals(id, that.id)
            && waitTimeout.equals(that.waitTimeout)
            && readMode == that.readMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, offset, maxRows, maxBytes, waitTimeout, readMode);
    }
}
