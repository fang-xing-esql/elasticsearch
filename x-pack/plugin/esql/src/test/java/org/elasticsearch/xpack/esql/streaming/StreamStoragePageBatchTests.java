/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

/**
 * {@link StreamStorage#getPageBatch}: the read of whole pages, in the in-memory store and in the default implementation on top of
 * {@code getPages}. The breaker check after each test fails if a batch does not release its pages.
 */
public class StreamStoragePageBatchTests extends AbstractStreamTestCase {

    private StreamStorage.PageBatch batch(StreamStorage storage, long fromOffset, int maxPages, long maxBytes) {
        return safeAwait(l -> storage.getPageBatch("s", fromOffset, maxPages, maxBytes, l));
    }

    private static List<Long> startOffsets(StreamStorage.PageBatch batch) {
        return batch.pages().stream().map(StreamStorage.StoredPage::startOffset).toList();
    }

    private static List<Long> values(StreamStorage.PageBatch batch) {
        List<Long> values = new ArrayList<>();
        for (StreamStorage.StoredPage stored : batch.pages()) {
            Page page = stored.page();
            LongBlock block = page.getBlock(0);
            for (int i = 0; i < page.getPositionCount(); i++) {
                values.add(block.getLong(i));
            }
        }
        return values;
    }

    public void testStartsWithThePageThatContainsTheOffset() {
        createStream("s").append(4).append(3).append(5).complete();
        // row 5 is in the second page, which starts at row 4, and the page is returned whole
        try (StreamStorage.PageBatch batch = batch(store, 5, 10, 0)) {
            assertThat(startOffsets(batch), equalTo(List.of(4L, 7L)));
            assertThat(values(batch), equalTo(range(4, 12)));
            assertThat(batch.nextOffset(), equalTo(12L));
            assertThat(batch.pages().get(0).endOffset(), equalTo(7L));
            assertThat(batch.pages().get(0).storedBytes(), greaterThan(0L));
        }
        // an offset at a page boundary starts with that page
        try (StreamStorage.PageBatch batch = batch(store, 7, 10, 0)) {
            assertThat(startOffsets(batch), equalTo(List.of(7L)));
        }
    }

    public void testMaxPages() {
        createStream("s").append(2).append(2).append(2).append(2).complete();
        try (StreamStorage.PageBatch batch = batch(store, 0, 3, 0)) {
            assertThat(startOffsets(batch), equalTo(List.of(0L, 2L, 4L)));
            assertThat(batch.nextOffset(), equalTo(6L));
        }
        try (StreamStorage.PageBatch batch = batch(store, 6, 3, 0)) {
            assertThat(startOffsets(batch), equalTo(List.of(6L)));
            assertThat(batch.nextOffset(), equalTo(8L));
        }
        Exception e = safeAwaitFailure(StreamStorage.PageBatch.class, l -> store.getPageBatch("s", 0, 0, 0, l));
        assertThat(e, instanceOf(IllegalArgumentException.class));
    }

    public void testMaxBytesEndsTheBatchWithThePageThatReachesIt() {
        createStream("s").append(2).append(2).append(2).complete();
        // one byte is reached by the first page, and at least one page is always returned
        try (StreamStorage.PageBatch batch = batch(store, 0, 10, 1)) {
            assertThat(startOffsets(batch), equalTo(List.of(0L)));
        }
        long onePage;
        try (StreamStorage.PageBatch batch = batch(store, 0, 1, 0)) {
            onePage = batch.pages().get(0).storedBytes();
        }
        try (StreamStorage.PageBatch batch = batch(store, 0, 10, onePage + 1)) {
            assertThat(startOffsets(batch), equalTo(List.of(0L, 2L)));
        }
    }

    public void testEmptyBatch() {
        Writer writer = createStream("s").append(3);
        try (StreamStorage.PageBatch batch = batch(store, 3, 10, 0)) {
            assertThat(batch.pages(), equalTo(List.of()));
            assertThat("an empty batch does not move", batch.nextOffset(), equalTo(3L));
        }
        // the storage does not wait: the caller asks again from the same offset
        writer.append(2);
        try (StreamStorage.PageBatch batch = batch(store, 3, 10, 0)) {
            assertThat(startOffsets(batch), equalTo(List.of(3L)));
        }
    }

    public void testUnknownStream() {
        Exception e = safeAwaitFailure(StreamStorage.PageBatch.class, l -> store.getPageBatch("unknown", 0, 1, 0, l));
        assertThat(e, instanceOf(ResourceNotFoundException.class));
    }

    public void testNoopStoreHasNoPages() {
        StreamStorage.PageBatch batch = safeAwait(l -> NoopStreamStore.INSTANCE.getPageBatch("s", 4, 1, 0, l));
        assertThat(batch.pages(), equalTo(List.of()));
        assertThat(batch.nextOffset(), equalTo(4L));
    }

    public void testBatchesReadTheWholeResultOnce() {
        Writer writer = createStream("s");
        int pages = between(1, 20);
        for (int i = 0; i < pages; i++) {
            writer.append(between(1, 10));
        }
        writer.complete();
        for (StreamStorage storage : List.of(store, new GetPagesOnlyStreamStorage(store))) {
            List<Long> values = new ArrayList<>();
            long offset = 0;
            while (true) {
                try (StreamStorage.PageBatch batch = batch(storage, offset, between(1, 5), randomBoolean() ? 0 : between(1, 2000))) {
                    if (batch.pages().isEmpty()) {
                        break;
                    }
                    assertThat("a batch starts where the previous one ended", batch.pages().get(0).startOffset(), equalTo(offset));
                    values.addAll(values(batch));
                    offset = batch.nextOffset();
                }
            }
            assertThat(values, equalTo(range(0, writer.rows)));
        }
    }

    public void testDefaultImplementationStartsAtTheOffset() {
        createStream("s").append(4).append(3).append(5).complete();
        // built on getPages, it cannot return the page whole: the first page starts at the requested row
        try (StreamStorage.PageBatch batch = batch(new GetPagesOnlyStreamStorage(store), 5, 10, 0)) {
            assertThat(startOffsets(batch), equalTo(List.of(5L, 7L)));
            assertThat(values(batch), equalTo(range(5, 12)));
            assertThat(batch.nextOffset(), equalTo(12L));
        }
        // pages that the window read past the bounds of the batch are released and read again by the next call
        try (StreamStorage.PageBatch batch = batch(new GetPagesOnlyStreamStorage(store), 0, 1, 0)) {
            assertThat(startOffsets(batch), equalTo(List.of(0L)));
            assertThat(batch.nextOffset(), equalTo(4L));
        }
    }
}
