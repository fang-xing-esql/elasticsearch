/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.esql.action.StreamNdjsonBody;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

/**
 * The stream GET in the read modes that read the storage a batch at a time, while the response is written and only when the client
 * asks for more: the batch mode, where a {@link StreamStoragePageProducer} feeds the publisher, and the driver mode, where a driver
 * with a {@link StreamStorageSourceOperator} does. Every test picks one of the two, and both must behave the same.
 * <p>
 * What all the read modes have in common is tested by the other tests of this package, which pick the mode randomly.
 */
public class StreamPageProducerTests extends AbstractStreamTestCase {

    @Before
    public void useBatchesOfOnePage() {
        readMode = randomFrom(GetStreamResultRequest.ReadMode.BATCH, GetStreamResultRequest.ReadMode.DRIVER);
        batchPages = 1;
    }

    private Writer stream(String id, int... pageRows) {
        Writer writer = createStream(id);
        for (int rows : pageRows) {
            writer.append(rows);
        }
        return writer;
    }

    public void testABatchIsReadOnlyWhenTheClientAsksForMore() throws IOException {
        stream("s", 3, 3, 3, 3).complete();
        RecordingStore recording = new RecordingStore();
        GetStreamResultResponse response = response(service(recording), request("s"));
        StreamNdjsonBody body = new StreamNdjsonBody(new FakeRestChannel(new FakeRestRequest(), true), response);
        try {
            assertThat("the first batch is read before the response starts", recording.pageReads(), equalTo(1));
            ChunkedRestResponseBodyPart part = body.firstPart();
            encode(part);
            assertThat("the first line reads nothing", recording.pageReads(), equalTo(1));
            for (int page = 1; page <= 4; page++) {
                part = nextPart(part);
                assertThat("page " + page + " needs batch " + page + " and no more", recording.pageReads(), equalTo(page));
                assertThat(part.isLastPart(), equalTo(false));
                encode(part);
                assertThat("writing a page reads nothing", recording.pageReads(), equalTo(page));
            }
            part = nextPart(part);
            assertThat(part.isLastPart(), equalTo(true));
            assertThat("a completed stream has a known end, no read comes back empty", recording.pageReads(), equalTo(4));
            assertThat("the row range read is not used in this mode", recording.getPagesCalls.get(), equalTo(0));
        } finally {
            body.close();
        }
    }

    public void testReadsWhileTheResponseIsWrittenNeverRunOnTheThreadThatAsks() {
        stream("s", 2, 2, 2, 2).complete();
        List<String> threads = Collections.synchronizedList(new ArrayList<>());
        StreamStorage storage = new DelegatingStreamStorage(store) {
            @Override
            public void getPageBatch(String streamId, long fromOffset, int maxPages, long maxBytes, ActionListener<PageBatch> listener) {
                threads.add(Thread.currentThread().getName());
                super.getPageBatch(streamId, fromOffset, maxPages, maxBytes, listener);
            }
        };
        Read read = read(service(storage), request("s"));
        assertThat(read.values(), equalTo(range(0, 8)));
        assertThat(threads.size(), equalTo(4));
        // the thread that asks for the next part is an HTTP thread in production, and the test thread here
        assertThat(threads.subList(1, 4), everyItem(containsString("[generic]")));
    }

    public void testMaxBytesStopsTheReads() {
        stream("s", 3, 3, 3, 3, 3).complete();
        RecordingStore recording = new RecordingStore();
        // a row such as [0] and its newline is 4 bytes: the limit is reached inside the first page
        Read read = read(service(recording), request("s", 0, GetStreamResultRequest.NO_LIMIT, 8));
        assertThat(read.values(), equalTo(range(0, 2)));
        assertThat(read.last().get("complete"), equalTo(false));
        assertThat(read.last().get("next_offset"), equalTo(2));
        assertThat("nothing is read after the limit", recording.pageReads(), equalTo(1));
    }

    public void testMaxRowsStopsTheReads() {
        stream("s", 3, 3, 3, 3, 3).complete();
        RecordingStore recording = new RecordingStore();
        Read read = read(service(recording), request("s", 1, 4, GetStreamResultRequest.NO_LIMIT));
        assertThat(read.values(), equalTo(range(1, 5)));
        assertThat(read.last().get("complete"), equalTo(false));
        assertThat(read.last().get("next_offset"), equalTo(5));
        assertThat("the two pages that hold the four rows", recording.pageReads(), equalTo(2));
    }

    public void testClosingTheBodyInTheMiddleStopsTheReads() throws IOException {
        stream("s", 3, 3, 3, 3).complete();
        RecordingStore recording = new RecordingStore();
        GetStreamResultResponse response = response(service(recording), request("s"));
        StreamNdjsonBody body = new StreamNdjsonBody(new FakeRestChannel(new FakeRestRequest(), true), response);
        ChunkedRestResponseBodyPart part = body.firstPart();
        encode(part);
        part = nextPart(part);
        // the client disconnects while the first page is written
        body.close();
        assertThat(recording.pageReads(), equalTo(1));
        // the breaker check after the test fails if a page was not released
    }

    public void testRunningStreamEndsWhenABatchComesBackEmpty() {
        stream("s", 3, 3);
        Read read = read(request("s"));
        assertThat(read.values(), equalTo(range(0, 6)));
        assertThat(read.first().get("query_status"), equalTo("running"));
        assertThat(read.last().get("complete"), equalTo(false));
        assertThat(read.last().get("next_offset"), equalTo(6));
    }

    public void testRowsStoredWhileTheResponseIsWrittenAreReturned() {
        Writer writer = stream("s", 3, 3);
        // the query adds a page and completes while the first page is written
        Read read = read(service(), request("s"), () -> writer.append(2).complete());
        assertThat(read.values(), equalTo(range(0, 8)));
        assertThat("the first line was written while the query was running", read.first().get("query_status"), equalTo("running"));
        assertThat("the last line has the status of the last read", read.last().get("complete"), equalTo(true));
        assertThat(read.last().get("next_offset"), equalTo(8));
    }

    public void testStreamFailingWhileTheResponseIsWrittenGivesTheErrorInTheLastLine() {
        Writer writer = stream("s", 3, 3);
        Read read = read(service(), request("s"), () -> writer.fail(new IllegalStateException("boom")));
        assertThat("the stored rows are still returned", read.values(), equalTo(range(0, 6)));
        assertThat(read.last().get("status"), equalTo(500));
        assertThat(read.last().get("complete"), equalTo(false));
        assertThat(read.last().get("next_offset"), equalTo(6));
        assertThat(read.last(), hasKey("error"));
    }

    @SuppressWarnings("unchecked")
    public void testStreamDeletedWhileTheResponseIsWritten() {
        stream("s", 3, 3, 3).complete();
        Read read = read(service(), request("s"), () -> awaitVoid(l -> store.delete("s", l)));
        // the status is already 200, so the not found error is in the last line, and the client knows where it stopped
        assertThat(read.values(), equalTo(range(0, 3)));
        assertThat(read.last().get("status"), equalTo(404));
        assertThat(read.last().get("is_partial"), equalTo(true));
        assertThat(read.last().get("complete"), equalTo(false));
        assertThat(read.last().get("next_offset"), equalTo(3));
        assertThat(((Map<String, Object>) read.last().get("error")).get("type"), equalTo("resource_not_found_exception"));
    }

    public void testStorageFailureWhileTheResponseIsWritten() {
        stream("s", 3, 3, 3).complete();
        AtomicInteger reads = new AtomicInteger();
        StreamStorage storage = new DelegatingStreamStorage(store) {
            @Override
            public void getPageBatch(String streamId, long fromOffset, int maxPages, long maxBytes, ActionListener<PageBatch> listener) {
                if (reads.incrementAndGet() == 3) {
                    listener.onFailure(new IllegalStateException("the storage is broken"));
                } else {
                    super.getPageBatch(streamId, fromOffset, maxPages, maxBytes, listener);
                }
            }
        };
        Read read = read(service(storage), request("s"));
        assertThat(read.values(), equalTo(range(0, 6)));
        assertThat(read.last().get("status"), equalTo(500));
        assertThat(read.last().get("complete"), equalTo(false));
        assertThat(read.last().get("next_offset"), equalTo(6));
        assertThat(read.last(), hasKey("error"));
    }

    public void testNotFoundOnTheFirstBatchIsAnHttpError() {
        stream("s", 3).complete();
        StreamStorage storage = new DelegatingStreamStorage(store) {
            @Override
            public void getPageBatch(String streamId, long fromOffset, int maxPages, long maxBytes, ActionListener<PageBatch> listener) {
                listener.onFailure(new ResourceNotFoundException(streamId));
            }
        };
        assertThat(readFailure(service(storage), request("s")).getClass(), equalTo(ResourceNotFoundException.class));
    }

    public void testEveryReadModeGivesTheSameResponse() {
        Writer writer = createStream("s");
        int pages = between(1, 12);
        for (int i = 0; i < pages; i++) {
            writer.append(between(1, 8));
        }
        if (randomBoolean()) {
            writer.complete();
        }
        batchPages = between(1, 4);
        for (int i = 0; i < 10; i++) {
            long offset = randomLongBetween(0, writer.rows);
            long maxRows = randomBoolean() ? GetStreamResultRequest.NO_LIMIT : between(1, 30);
            long maxBytes = randomBoolean() ? GetStreamResultRequest.NO_LIMIT : between(1, 200);
            readMode = GetStreamResultRequest.ReadMode.PAGES;
            Read fromPages = read(request("s", offset, maxRows, maxBytes));
            for (GetStreamResultRequest.ReadMode mode : GetStreamResultRequest.ReadMode.values()) {
                readMode = mode;
                assertThat(mode.toString(), read(request("s", offset, maxRows, maxBytes)), equalTo(fromPages));
            }
        }
    }

    public void testStorageWithoutItsOwnBatchRead() {
        // a storage that only has getPages still works in the batch mode, through the default getPageBatch
        stream("s", 4, 3, 5).complete();
        StreamStorage storage = new GetPagesOnlyStreamStorage(store);
        Read read = read(service(storage), request("s", 2, GetStreamResultRequest.NO_LIMIT, GetStreamResultRequest.NO_LIMIT));
        assertThat(read.values(), equalTo(range(2, 12)));
        assertThat(read.last().get("complete"), equalTo(true));
        assertThat(read.last(), not(hasKey("error")));
    }

}
