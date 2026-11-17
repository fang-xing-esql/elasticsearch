/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.rest.ChunkedRestResponseBodyPart;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xpack.esql.action.StreamNdjsonBody;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

/**
 * Slicing and limits of the stream GET. The async API returns the whole result, so these have no async counterpart.
 * Rows are one-element arrays whose value is the row number, so a row such as {@code [7]} followed by a newline is 4 bytes.
 */
public class StreamNdjsonBodyTests extends AbstractStreamTestCase {

    private Writer completedStream(String id, int... pageRows) {
        Writer writer = createStream(id);
        for (int rows : pageRows) {
            writer.append(rows);
        }
        return writer.complete();
    }

    @SuppressWarnings("unchecked")
    public void testFullReadOfACompletedStream() {
        completedStream("s", 4, 3, 5);
        Read read = read(request("s"));
        assertThat(read.values(), equalTo(range(0, 12)));
        assertThat(read.first().get("id"), equalTo("s"));
        assertThat(read.first().get("query_status"), equalTo("completed"));
        assertThat(read.first().get("columns"), equalTo(List.of(Map.of("name", "n", "type", "long"))));
        assertThat(
            read.last(),
            equalTo(Map.of("status", 200, "is_partial", false, "warnings", List.of(), "complete", true, "next_offset", 12))
        );
    }

    public void testOffsets() {
        completedStream("s", 4, 3, 5);
        // a page start, inside a page, the last row, the total (nothing left)
        for (long offset : new long[] { 0, 1, 4, 5, 7, 11, 12 }) {
            Read read = read(request("s", offset, GetStreamResultRequest.NO_LIMIT, GetStreamResultRequest.NO_LIMIT));
            assertThat("offset " + offset, read.values(), equalTo(range(offset, 12)));
            assertThat(read.last().get("complete"), equalTo(true));
            assertThat(read.last().get("next_offset"), equalTo(12));
        }
    }

    public void testOffsetBeyondTheEndOfACompletedStreamIsABadRequest() {
        completedStream("s", 4, 3, 5);
        Exception e = readFailure(service(), request("s", 13, GetStreamResultRequest.NO_LIMIT, GetStreamResultRequest.NO_LIMIT));
        assertThat(e, instanceOf(IllegalArgumentException.class));
        assertThat(e.getMessage(), containsString("offset [13] is beyond the end of the result [12]"));
    }

    public void testMaxRows() {
        completedStream("s", 4, 3, 5);
        for (long maxRows : new long[] { 1, 3, 4, 5, 11, 12, 13, 1000 }) {
            Read read = read(request("s", 0, maxRows, GetStreamResultRequest.NO_LIMIT));
            long expected = Math.min(maxRows, 12);
            assertThat("max_rows " + maxRows, read.values(), equalTo(range(0, expected)));
            assertThat(read.last().get("next_offset"), equalTo((int) expected));
            // complete only when the end of the result was reached, including when max_rows is exactly the rows left
            assertThat("max_rows " + maxRows, read.last().get("complete"), equalTo(maxRows >= 12));
        }
    }

    public void testMaxRowsWithAnOffsetAcrossPages() {
        completedStream("s", 4, 3, 5);
        Read read = read(request("s", 2, 7, GetStreamResultRequest.NO_LIMIT));
        assertThat(read.values(), equalTo(range(2, 9)));
        assertThat(read.last().get("next_offset"), equalTo(9));
        assertThat(read.last().get("complete"), equalTo(false));
    }

    public void testStatusOnlyReadsNoPages() {
        completedStream("s", 4, 3, 5);
        RecordingStore recording = new RecordingStore();
        StreamResultsService service = service(recording);
        for (GetStreamResultRequest request : List.of(
            request("s", 0, 0, GetStreamResultRequest.NO_LIMIT),
            request("s", 0, GetStreamResultRequest.NO_LIMIT, 0),
            request("s", 0, 0, 0)
        )) {
            Read read = read(service, request);
            assertThat(read.rows(), equalTo(List.of()));
            assertThat(read.first().get("query_status"), equalTo("completed"));
            assertThat(read.last().get("next_offset"), equalTo(0));
            assertThat(read.last().get("complete"), equalTo(false));
        }
        assertThat("no page is read for a status only request", recording.getPagesCalls.get(), equalTo(0));
    }

    public void testMaxBytesStopsAtTheFirstRowBoundaryAtOrPastTheLimit() {
        completedStream("s", 4, 3, 5);
        // the rows [0] to [9] are 4 bytes each with their newline
        Read read = read(request("s", 0, GetStreamResultRequest.NO_LIMIT, 10));
        assertThat(read.values(), equalTo(range(0, 3)));
        assertThat(read.last().get("next_offset"), equalTo(3));
        assertThat(read.last().get("complete"), equalTo(false));

        // exactly on a row boundary
        read = read(request("s", 0, GetStreamResultRequest.NO_LIMIT, 8));
        assertThat(read.values(), equalTo(range(0, 2)));
    }

    public void testARowLargerThanMaxBytesIsStillWritten() {
        completedStream("s", 4, 3, 5);
        Read read = read(request("s", 0, GetStreamResultRequest.NO_LIMIT, 1));
        assertThat(
            "a limit that is not reached yet always lets a row through, so the read makes progress",
            read.values(),
            equalTo(range(0, 1))
        );
        assertThat(read.last().get("next_offset"), equalTo(1));
    }

    public void testBothLimitsTheFirstOneReachedWins() {
        completedStream("s", 4, 3, 5);
        assertThat(read(request("s", 0, 2, 1000)).values(), equalTo(range(0, 2)));
        assertThat(read(request("s", 0, 100, 5)).values(), equalTo(range(0, 2)));
    }

    public void testNoLimitsReturnsEverythingInOneRead() {
        completedStream("s", 7, 7, 7, 7, 7);
        RecordingStore recording = new RecordingStore();
        Read read = read(service(recording), request("s"));
        assertThat(read.values(), equalTo(range(0, 35)));
        assertThat(read.last().get("complete"), equalTo(true));
        assertThat(read.last().get("next_offset"), equalTo(35));
        assertThat("the whole result comes from one getPages call", recording.getPagesCalls.get(), equalTo(1));
    }

    public void testOnlyMaxRowsSetHasNoByteCap() {
        Writer writer = createStream("s");
        for (int i = 0; i < 20; i++) {
            writer.append(50);
        }
        writer.complete();
        Read read = read(service(store), request("s", 0, 777, GetStreamResultRequest.NO_LIMIT));
        assertThat(read.values(), equalTo(range(0, 777)));
    }

    public void testOnlyMaxBytesSetHasNoRowCap() {
        Writer writer = createStream("s");
        for (int i = 0; i < 20; i++) {
            writer.append(50);
        }
        writer.complete();
        // 1000 rows of 4 to 6 bytes: a byte limit well above any row cap a default would impose
        Read read = read(service(store), request("s", 0, GetStreamResultRequest.NO_LIMIT, 1_000_000));
        assertThat(read.values(), equalTo(range(0, 1000)));
        assertThat(read.last().get("complete"), equalTo(true));
    }

    public void testResumeLoopGivesTheWholeResultOnce() {
        completedStream("s", 5, 1, 9, 2, 8);
        List<Long> all = new ArrayList<>();
        long offset = 0;
        boolean complete = false;
        int iterations = 0;
        while (complete == false) {
            long maxRows = randomBoolean() ? randomLongBetween(1, 7) : GetStreamResultRequest.NO_LIMIT;
            long maxBytes = randomBoolean() ? randomLongBetween(1, 30) : GetStreamResultRequest.NO_LIMIT;
            Read read = read(request("s", offset, maxRows, maxBytes));
            all.addAll(read.values());
            offset = ((Number) read.last().get("next_offset")).longValue();
            complete = (boolean) read.last().get("complete");
            assertThat("every call makes progress", iterations++ < 100, equalTo(true));
        }
        assertThat(all, equalTo(range(0, 25)));
    }

    public void testRunningStreamReturnsWhatIsStored() {
        createStream("s").append(4).append(3);
        Read read = read(request("s"));
        assertThat(read.values(), equalTo(range(0, 7)));
        assertThat(read.first().get("query_status"), equalTo("running"));
        assertThat(read.last().get("complete"), equalTo(false));
        assertThat(read.last().get("next_offset"), equalTo(7));
        assertThat(read.last().get("is_partial"), equalTo(false));
    }

    public void testRunningStreamWithNoRowsAtTheOffset() {
        createStream("s").append(4);
        Read read = read(request("s", 4, GetStreamResultRequest.NO_LIMIT, GetStreamResultRequest.NO_LIMIT));
        assertThat(read.rows(), equalTo(List.of()));
        assertThat(read.last().get("next_offset"), equalTo(4));
        assertThat(read.last().get("complete"), equalTo(false));
    }

    public void testRunningStreamWithAHugeOffsetDoesNotOverflow() {
        createStream("s").append(4);
        Read read = read(request("s", Long.MAX_VALUE - 1, GetStreamResultRequest.NO_LIMIT, GetStreamResultRequest.NO_LIMIT));
        assertThat(read.rows(), equalTo(List.of()));
        assertThat(read.last().get("complete"), equalTo(false));
    }

    @SuppressWarnings("unchecked")
    public void testFailedStreamGivesTheStoredRowsAndTheErrorInTheLastLine() {
        createStream("s").append(4).append(3).fail(new IllegalStateException("boom"));
        Read read = read(request("s"));
        assertThat(read.values(), equalTo(range(0, 7)));
        assertThat(read.first().get("query_status"), equalTo("failed"));
        assertThat(read.last().get("status"), equalTo(500));
        assertThat(read.last().get("complete"), equalTo(false));
        assertThat(read.last().get("is_partial"), equalTo(true));
        assertThat(read.last().get("next_offset"), equalTo(7));
        Map<String, Object> error = (Map<String, Object>) read.last().get("error");
        assertThat(error.get("reason"), equalTo("boom"));
    }

    public void testFailedStreamWithNoRowsAtTheOffsetIsAnHttpError() {
        createStream("s").append(4).fail(new IllegalStateException("boom"));
        Exception e = readFailure(service(), request("s", 4, GetStreamResultRequest.NO_LIMIT, GetStreamResultRequest.NO_LIMIT));
        assertThat(e, instanceOf(IllegalStateException.class));
        assertThat(e.getMessage(), equalTo("boom"));
    }

    public void testFailedStreamStatusOnlyIsNotAnHttpError() {
        createStream("s").append(4).fail(new IllegalStateException("boom"));
        Read read = read(request("s", 0, 0, GetStreamResultRequest.NO_LIMIT));
        assertThat(read.first().get("query_status"), equalTo("failed"));
        assertThat(read.last(), hasKey("error"));
    }

    public void testCancelledStream() {
        createStream("s").append(4);
        StreamStorage cancelled = new DelegatingStreamStorage(store) {
            @Override
            public void getMetadata(String streamId, ActionListener<StreamMetadata> listener) {
                super.getMetadata(
                    streamId,
                    listener.map(
                        m -> new StreamMetadata(
                            m.streamId(),
                            m.ttl(),
                            m.taskId(),
                            m.headers(),
                            m.columns(),
                            StreamMetadata.Status.CANCELED,
                            m.warnings(),
                            null
                        )
                    )
                );
            }
        };
        Read read = read(service(cancelled), request("s"));
        assertThat(read.values(), equalTo(range(0, 4)));
        assertThat(read.first().get("query_status"), equalTo("cancelled"));
        assertThat(read.last().get("complete"), equalTo(false));
        assertThat(read.last(), not(hasKey("error")));
    }

    public void testWarningsAreReturnedInTheLastLine() {
        completedStream("s", 4);
        StreamStorage withWarnings = new DelegatingStreamStorage(store) {
            @Override
            public void getMetadata(String streamId, ActionListener<StreamMetadata> listener) {
                super.getMetadata(
                    streamId,
                    listener.map(
                        m -> new StreamMetadata(
                            m.streamId(),
                            m.ttl(),
                            m.taskId(),
                            m.headers(),
                            m.columns(),
                            m.status(),
                            List.of("w1", "w2"),
                            m.completionInfo()
                        )
                    )
                );
            }
        };
        Read read = read(service(withWarnings), request("s"));
        assertThat(read.last().get("warnings"), equalTo(List.of("w1", "w2")));
    }

    public void testPartialCompletedStream() {
        Writer writer = createStream("s").append(4);
        awaitVoid(l -> store.markComplete("s", true, writer.rows, writer.pages, l));
        Read read = read(request("s"));
        assertThat(read.last().get("is_partial"), equalTo(true));
        assertThat(read.last().get("complete"), equalTo(true));
    }

    public void testStreamCompletingBetweenTheMetadataAndTheReadIsNotReportedCompleteEarly() {
        Writer writer = createStream("s").append(4);
        AtomicBoolean completed = new AtomicBoolean();
        StreamStorage completingStore = new DelegatingStreamStorage(store) {
            @Override
            public void getPages(String streamId, long offset, long rows, ActionListener<List<Page>> listener) {
                // the query finishes after the status was read as RUNNING and before the pages are read
                if (completed.compareAndSet(false, true)) {
                    writer.append(3).complete();
                }
                super.getPages(streamId, offset, rows, listener);
            }
        };
        StreamResultsService service = service(completingStore);
        Read first = read(service, request("s"));
        assertThat(first.values(), equalTo(range(0, 7)));
        assertThat("the status was read as running, so the end of the result is not claimed", first.last().get("complete"), equalTo(false));
        assertThat(first.last().get("next_offset"), equalTo(7));

        Read second = read(service, request("s", 7, GetStreamResultRequest.NO_LIMIT, GetStreamResultRequest.NO_LIMIT));
        assertThat(second.rows(), equalTo(List.of()));
        assertThat(second.last().get("complete"), equalTo(true));
    }

    public void testOneReadPerGetAndNothingIsReadWhileWriting() throws IOException {
        completedStream("s", 3, 3, 3);
        RecordingStore recording = new RecordingStore();
        GetStreamResultResponse response = response(service(recording), request("s"));
        FakeRestChannel channel = new FakeRestChannel(new FakeRestRequest(), true);
        StreamNdjsonBody body = new StreamNdjsonBody(channel, response);
        try {
            assertThat("the pages were read before the response started", recording.getPagesCalls.get(), equalTo(1));
            ChunkedRestResponseBodyPart part = body.firstPart();
            while (part.isLastPart() == false) {
                encode(part);
                AtomicReference<ChunkedRestResponseBodyPart> next = new AtomicReference<>();
                part.getNextPart(ActionListener.wrap(next::set, e -> fail(e)));
                part = next.get();
            }
            encode(part);
            assertThat("writing the response reads nothing more", recording.getPagesCalls.get(), equalTo(1));
        } finally {
            body.close();
        }
    }

    public void testClosingTheBodyReleasesThePagesThatWereNotWritten() {
        completedStream("s", 3, 3, 3);
        GetStreamResultResponse response = response(service(), request("s"));
        StreamNdjsonBody body = new StreamNdjsonBody(new FakeRestChannel(new FakeRestRequest(), true), response);
        body.close();
        // the breaker check after the test fails if a page was not released
    }

    public void testClosingTheResponseThatWasNeverUsedReleasesItsPages() {
        completedStream("s", 3, 3, 3);
        response(service(), request("s")).close();
    }

    public void testTheSingleReadAsksForTheRestOfTheResultWithoutOverflowing() {
        completedStream("s", 5, 5, 5);
        // no max_rows: everything from the offset, and offset + rows must not overflow in the storage
        RecordingStore recording = new RecordingStore();
        read(service(recording), request("s", 5, GetStreamResultRequest.NO_LIMIT, GetStreamResultRequest.NO_LIMIT));
        assertThat(recording.requestedRows, equalTo(List.of(Long.MAX_VALUE - 5)));

        // max_rows: exactly that many
        recording = new RecordingStore();
        read(service(recording), request("s", 0, 3, GetStreamResultRequest.NO_LIMIT));
        assertThat(recording.requestedRows, equalTo(List.of(3L)));

        // max_rows too large for the offset: clamped
        recording = new RecordingStore();
        read(service(recording), request("s", 5, Long.MAX_VALUE, GetStreamResultRequest.NO_LIMIT));
        assertThat(recording.requestedRows, equalTo(List.of(Long.MAX_VALUE - 5)));
    }

    public void testStoredRowsAreWrittenOnePerLine() {
        completedStream("s", 3);
        // the rows are bare arrays, one per line, with no wrapper
        Read read = read(request("s"));
        assertThat(read.rows(), equalTo(List.of(List.of(0), List.of(1), List.of(2))));
    }
}
