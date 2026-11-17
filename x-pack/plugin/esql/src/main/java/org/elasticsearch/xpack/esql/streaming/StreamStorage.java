/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public interface StreamStorage {

    void create(
        String streamId,
        TaskId taskId,
        Map<String, String> headers,
        List<ColumnInfo> columns,
        long expirationMillis,
        ActionListener<Void> listener
    );

    void extendTtl(String streamId, long expirationMillis, ActionListener<Void> listener);

    void appendPage(String streamId, long offset, Page page, ActionListener<Void> listener);

    void markComplete(String streamId, boolean isPartial, long totalRows, long totalPages, ActionListener<Void> listener);

    void markFailed(String streamId, Exception exception, ActionListener<Void> listener);

    void getMetadata(String streamId, ActionListener<StreamMetadata> listener);

    void getPages(String streamId, long offset, long rows, ActionListener<List<Page>> listener);

    /**
     * The number of rows per requested page that the default {@link #getPageBatch} asks {@link #getPages} for. A storage that knows
     * its page boundaries overrides {@link #getPageBatch} and does not use it.
     */
    int DEFAULT_BATCH_ROWS_PER_PAGE = 1000;

    /**
     * Reads a few whole pages in order, starting with the page that contains row {@code fromOffset}. It exists so that a reader can
     * go through a large result a bounded amount at a time, without reading a stored page twice and without asking for rows it will
     * discard, which {@link #getPages} cannot do because it works on row ranges.
     * <p>
     * Pages are not trimmed: the first page may start before {@code fromOffset}, and the caller trims it. The next call starts at
     * {@link PageBatch#nextOffset()}, which is the end of the last page. The batch is empty when no stored page covers
     * {@code fromOffset} (yet). The storage never waits for pages.
     * <p>
     * This default is built on {@link #getPages} with a window of rows, so that every storage supports the call. It returns pages
     * that start exactly at {@code fromOffset}, and it may read a stored page that spans two windows twice.
     *
     * @param maxPages the largest number of pages to return, at least 1
     * @param maxBytes when positive, the batch ends with the page that brings its stored bytes to this value or more. At least one
     *                 page is returned when one is available.
     * @param listener receives the batch. The caller owns its pages and must close it.
     */
    default void getPageBatch(String streamId, long fromOffset, int maxPages, long maxBytes, ActionListener<PageBatch> listener) {
        if (maxPages < 1) {
            listener.onFailure(new IllegalArgumentException("[maxPages] must be at least 1 but was [" + maxPages + "]"));
            return;
        }
        long rows = Math.min((long) maxPages * DEFAULT_BATCH_ROWS_PER_PAGE, Long.MAX_VALUE - fromOffset);
        getPages(streamId, fromOffset, rows, listener.map(pages -> {
            List<StoredPage> stored = new ArrayList<>();
            long offset = fromOffset;
            long bytes = 0;
            boolean full = false;
            for (Page page : pages) {
                if (full) {
                    // Read by the window but past the bounds of the batch: it is read again by the next call.
                    page.releaseBlocks();
                    continue;
                }
                long pageBytes = page.ramBytesUsedByBlocks();
                stored.add(new StoredPage(offset, page, pageBytes));
                offset += page.getPositionCount();
                bytes += pageBytes;
                full = stored.size() >= maxPages || (maxBytes > 0 && bytes >= maxBytes);
            }
            return new PageBatch(fromOffset, stored);
        }));
    }

    /**
     * A stored page and where it is in the result.
     *
     * @param startOffset the position of the first row of the page in the whole result
     * @param storedBytes the size of the page in the storage. It is a hint for how much was read, not the size of the rendered rows.
     */
    record StoredPage(long startOffset, Page page, long storedBytes) {
        /** The position after the last row of the page. */
        public long endOffset() {
            return startOffset + page.getPositionCount();
        }
    }

    /**
     * The result of {@link #getPageBatch}. Whoever holds it owns the pages and must close it, or take the pages and stop using it.
     *
     * @param fromOffset the offset that was asked for
     * @param pages      consecutive whole pages, in order. The first one contains {@code fromOffset}.
     */
    record PageBatch(long fromOffset, List<StoredPage> pages) implements Releasable {
        /** Where the next call starts: the end of the last page, or the requested offset when the batch is empty. */
        public long nextOffset() {
            return pages.isEmpty() ? fromOffset : pages.get(pages.size() - 1).endOffset();
        }

        @Override
        public void close() {
            for (StoredPage page : pages) {
                page.page().releaseBlocks();
            }
        }
    }

    void delete(String streamId, ActionListener<Void> listener);
}
