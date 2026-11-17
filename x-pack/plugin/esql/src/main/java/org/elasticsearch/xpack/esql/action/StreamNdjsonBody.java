/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.streaming.GetStreamResultRequest;
import org.elasticsearch.xpack.esql.streaming.GetStreamResultResponse;
import org.elasticsearch.xpack.esql.streaming.StreamMetadata;
import org.elasticsearch.xpack.esql.streaming.StreamPages;

import java.io.IOException;
import java.io.OutputStream;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE;

/**
 * Writes the result of a stream read as NDJSON, one line per logical unit:
 * <ul>
 *   <li>First line: {@code {"id":..., "columns":[...], "query_status":"running|completed|failed|cancelled"}}</li>
 *   <li>One line per row: a JSON array with the values in column order, {@code null} where there is no value</li>
 *   <li>Last line: {@code {"status":200,"is_partial":false,"warnings":[...],"complete":false,"next_offset":N}}, with an {@code error}
 *       object when the query failed</li>
 * </ul>
 * <p>
 * The pages were read from the storage, in one call, before the response started. The body is pull based: the HTTP layer asks for the
 * next part when it has written the previous one, so a slow client slows the writing and nothing is buffered ahead of the socket.
 * <p>
 * {@code max_rows} and {@code max_bytes} are counted while rows are written, and the first one reached ends the response at a row
 * boundary. When a limit is not set there is no limit. Bytes count the rows only, including their newline, not the first and last
 * lines. A row is always written when a limit is not yet reached, so a row larger than {@code max_bytes} is still written.
 * <p>
 * Pages are owned by this body until their part is fully written, and are released by the part, by {@link #close()} or when a limit
 * is reached.
 */
public final class StreamNdjsonBody implements Releasable {

    private static final Logger logger = LogManager.getLogger(StreamNdjsonBody.class);
    private static final String NDJSON_CONTENT_TYPE = "application/x-ndjson";
    private static final byte[] NEWLINE = new byte[] { '\n' };

    private final RestChannel channel;
    private final GetStreamResultRequest request;
    private final List<ColumnInfoImpl> columns;

    private final Object lock = new Object();
    /** Pages read but not yet handed to a part. Guarded by {@code lock}. */
    private final ArrayDeque<Page> pending = new ArrayDeque<>();
    /** The page of the part being written. Guarded by {@code lock}. */
    private Page inFlight;
    private boolean closed;

    private final StreamMetadata metadata;
    private volatile long rowsWritten;
    private volatile long bytesWritten;

    public StreamNdjsonBody(RestChannel channel, GetStreamResultResponse response) {
        this.channel = channel;
        this.request = response.request();
        this.metadata = response.metadata();
        this.columns = new ArrayList<>(metadata.columns().size());
        for (ColumnInfo column : metadata.columns()) {
            // The stored columns are written by the ES|QL plugin, so they are always ColumnInfoImpl, which the value converters need.
            if (column instanceof ColumnInfoImpl impl) {
                columns.add(impl);
            } else {
                response.close();
                throw new IllegalStateException("unexpected column implementation [" + column.getClass().getName() + "]");
            }
        }
        StreamPages read = response.takePages();
        synchronized (lock) {
            pending.addAll(read.pages());
        }
    }

    public ChunkedRestResponseBodyPart firstPart() {
        return new ColumnsPart();
    }

    private boolean limitReached() {
        return (request.hasRowLimit() && rowsWritten >= request.maxRows())
            || (request.hasByteLimit() && bytesWritten >= request.maxBytes());
    }

    /** Produces the part after the one just written: the next page, or the last line when no page is left or a limit was reached. */
    private void next(ActionListener<ChunkedRestResponseBodyPart> listener) {
        try {
            if (request.statusOnly() || limitReached()) {
                releasePending();
                listener.onResponse(new FooterPart());
                return;
            }
            Page page;
            synchronized (lock) {
                if (closed) {
                    listener.onFailure(new IllegalStateException("the response was closed"));
                    return;
                }
                page = pending.poll();
                inFlight = page;
            }
            listener.onResponse(page != null ? new PagePart(page) : new FooterPart());
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void releasePending() {
        synchronized (lock) {
            for (Page page : pending) {
                page.releaseBlocks();
            }
            pending.clear();
        }
    }

    private void releaseInFlight(Page page) {
        synchronized (lock) {
            if (inFlight == page) {
                inFlight = null;
                page.releaseBlocks();
            }
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            closed = true;
            for (Page page : pending) {
                page.releaseBlocks();
            }
            pending.clear();
            if (inFlight != null) {
                inFlight.releaseBlocks();
                inFlight = null;
            }
        }
    }

    private static String queryStatus(StreamMetadata.Status status) {
        return switch (status) {
            case RUNNING -> "running";
            case COMPLETE -> "completed";
            case FAILED -> "failed";
            case CANCELED -> "cancelled";
        };
    }

    private abstract static class SingleLinePart implements ChunkedRestResponseBodyPart {
        private boolean encoded = false;

        abstract void writeLine(XContentBuilder builder) throws IOException;

        @Override
        public boolean isPartComplete() {
            return encoded;
        }

        @Override
        public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
            final RecyclerBytesStreamOutput out = new RecyclerBytesStreamOutput(recycler);
            try {
                try (XContentBuilder builder = XContentFactory.jsonBuilder(new OutputStream() {
                    @Override
                    public void write(int b) throws IOException {
                        out.write(b);
                    }

                    @Override
                    public void write(byte[] b, int off, int len) throws IOException {
                        out.write(b, off, len);
                    }
                })) {
                    writeLine(builder);
                }
                out.write(NEWLINE);
                encoded = true;
                return out.moveToBytesReference();
            } catch (Exception e) {
                encoded = true;
                IOUtils.closeWhileHandlingException(out);
                throw e;
            }
        }

        @Override
        public String getResponseContentTypeString() {
            return NDJSON_CONTENT_TYPE;
        }
    }

    private final class ColumnsPart extends SingleLinePart {
        @Override
        void writeLine(XContentBuilder builder) throws IOException {
            builder.startObject();
            builder.field("id", request.id());
            builder.startArray("columns");
            for (ColumnInfoImpl column : columns) {
                column.toXContent(builder, channel.request());
            }
            builder.endArray();
            builder.field("query_status", queryStatus(metadata.status()));
            builder.endObject();
        }

        @Override
        public boolean isLastPart() {
            return false;
        }

        @Override
        public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
            next(listener);
        }
    }

    private final class FooterPart extends SingleLinePart {

        @Override
        void writeLine(XContentBuilder builder) throws IOException {
            StreamMetadata meta = metadata;
            StreamMetadata.StreamCompletionInfo completion = meta.completionInfo();
            long nextOffset = request.offset() + rowsWritten;

            Exception error = null;
            if (meta.status() == StreamMetadata.Status.FAILED && completion != null) {
                error = completion.exception();
            }
            // All the stored pages were written, no limit stopped the response, and still rows are missing: the storage lost pages.
            if (error == null
                && request.statusOnly() == false
                && limitReached() == false
                && meta.status() == StreamMetadata.Status.COMPLETE
                && completion != null
                && nextOffset < completion.rows()) {
                error = new IllegalStateException(
                    "stream ["
                        + request.id()
                        + "] is complete with ["
                        + completion.rows()
                        + "] rows but only has rows up to ["
                        + nextOffset
                        + "]"
                );
            }
            boolean complete = error == null
                && meta.status() == StreamMetadata.Status.COMPLETE
                && completion != null
                && nextOffset >= completion.rows();
            boolean partial = error != null
                || (meta.status() == StreamMetadata.Status.COMPLETE && completion != null && completion.partial());
            int status = error != null ? ExceptionsHelper.status(error).getStatus() : RestStatus.OK.getStatus();

            builder.startObject();
            builder.field("status", status);
            builder.field("is_partial", partial);
            builder.array("warnings", meta.warnings().toArray(String[]::new));
            builder.field("complete", complete);
            builder.field("next_offset", nextOffset);
            if (error != null) {
                ElasticsearchException.generateFailureXContent(builder, errorParams(status), error, channel.detailedErrorsEnabled());
            }
            builder.endObject();
        }

        @Override
        public boolean isLastPart() {
            return true;
        }

        @Override
        public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
            assert false : "the last line has no continuation";
            listener.onFailure(new IllegalStateException("no continuations available"));
        }
    }

    private final class PagePart implements ChunkedRestResponseBodyPart {
        private final Page page;
        private RecyclerBytesStreamOutput target;
        private final OutputStream out = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                target.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                target.write(b, off, len);
            }
        };
        private PositionToXContent[] converters;
        private BytesRef scratch;
        private int nextRow = 0;
        private boolean encoded = false;

        PagePart(Page page) {
            this.page = page;
        }

        @Override
        public boolean isPartComplete() {
            return encoded;
        }

        @Override
        public boolean isLastPart() {
            return false;
        }

        @Override
        public void getNextPart(ActionListener<ChunkedRestResponseBodyPart> listener) {
            next(listener);
        }

        @Override
        public ReleasableBytesReference encodeChunk(int sizeHint, Recycler<BytesRef> recycler) throws IOException {
            final RecyclerBytesStreamOutput chunkStream = new RecyclerBytesStreamOutput(recycler);
            target = chunkStream;
            try {
                final int rowCount = page.getPositionCount();
                final int colCount = columns.size();
                if (converters == null) {
                    scratch = new BytesRef();
                    converters = new PositionToXContent[colCount];
                    for (int c = 0; c < colCount; c++) {
                        // The time zone of the query is not stored with the stream, so dates are written in UTC.
                        converters[c] = PositionToXContent.positionToXContent(columns.get(c), page.getBlock(c), ZoneOffset.UTC, scratch);
                    }
                }
                // Rows are written until the chunk reaches sizeHint or a limit is reached. The limits are checked before each row
                // for max_rows and after each row for max_bytes, so at least one row is written whenever no limit is reached yet.
                while (nextRow < rowCount && limitReached() == false) {
                    int before = chunkStream.size();
                    try (XContentBuilder builder = XContentFactory.jsonBuilder(out)) {
                        builder.startArray();
                        for (int col = 0; col < colCount; col++) {
                            converters[col].positionToXContent(builder, channel.request(), nextRow);
                        }
                        builder.endArray();
                    }
                    chunkStream.write(NEWLINE);
                    nextRow++;
                    rowsWritten++;
                    bytesWritten += chunkStream.size() - before;
                    if (chunkStream.size() >= sizeHint) {
                        break;
                    }
                }
                if (nextRow == rowCount || limitReached()) {
                    // Rows after a reached limit are dropped with the page.
                    encoded = true;
                }
                final var result = chunkStream.moveToBytesReference();
                target = null;
                return result;
            } catch (Exception e) {
                logger.error("failure encoding page chunk", e);
                encoded = true;
                IOUtils.closeWhileHandlingException(chunkStream);
                target = null;
                throw e;
            } finally {
                if (encoded) {
                    releaseInFlight(page);
                }
            }
        }

        @Override
        public String getResponseContentTypeString() {
            return NDJSON_CONTENT_TYPE;
        }
    }

    private ToXContent.Params errorParams(int statusCode) {
        ToXContent.Params params = channel.request();
        if (statusCode != RestStatus.UNAUTHORIZED.getStatus() && params.paramAsBoolean("error_trace", RestController.ERROR_TRACE_DEFAULT)) {
            params = new ToXContent.DelegatingMapParams(Map.of(REST_EXCEPTION_SKIP_STACK_TRACE, "false"), params);
        }
        return params;
    }
}
