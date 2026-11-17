/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.PageStreamPublisher;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * Feeds the pages of a stored stream to a {@link PageStreamPublisher}, the role {@code StreamingPageOperator} has for a running query.
 * The stream GET uses the publisher the same way the streaming POST does: the response body subscribes to it and asks for one page
 * each time the HTTP layer is ready for the next part.
 * <p>
 * The producer adds a page only when the publisher says it is writable, which is when the subscriber asked for a page that is not
 * buffered yet. So nothing is added, and nothing more is read from the storage, ahead of the client.
 * <p>
 * The subclasses are the two read modes of the GET, see {@link GetStreamResultRequest.ReadMode}. They differ only in where the pages
 * after the first read come from.
 * <p>
 * The producer owns the pages it holds until it adds them to the publisher, and releases them when it is closed, when the subscriber
 * cancels or when it fails.
 */
public abstract class StreamPageProducer implements StreamResultPages {

    private final PageStreamPublisher publisher = new PageStreamPublisher();
    private final PageStreamPublisher.Producer handle = publisher.registerProducer();
    private final Executor executor;

    /** Pages read but not yet added to the publisher. Guarded by {@code this}. */
    private final ArrayDeque<Page> ready = new ArrayDeque<>();
    /** The first read, until {@link #start()} takes its pages. Guarded by {@code this}. */
    private StreamPages first;
    /** Set when the producer must not add or read anything more. Guarded by {@code this}. */
    private boolean stopped;
    /** The metadata of the latest read. */
    private volatile StreamMetadata metadata;
    /** Where the next read from the storage starts. Only used by the pump, one step at a time. */
    private volatile long nextOffset;
    /** Rows that may still be added before {@code max_rows} is reached. Only used by the pump. */
    private volatile long rowsLeft;
    /** Whether the storage may have more pages. Only used by the pump. */
    private volatile boolean mayHaveMore;

    /**
     * @param executor runs the reads from the storage and the steps that follow them. The publisher reports that it is writable on the
     *                 thread that asked for a page, which is an HTTP thread, so the producer always continues on this executor.
     * @param first    the result of the first read, made before the response starts. The producer takes its pages.
     * @param maxRows  the largest number of rows to add, or {@link GetStreamResultRequest#NO_LIMIT}
     */
    protected StreamPageProducer(Executor executor, StreamPages first, long maxRows) {
        this.executor = executor;
        this.rowsLeft = maxRows == GetStreamResultRequest.NO_LIMIT ? Long.MAX_VALUE : maxRows;
        this.first = first;
        this.metadata = first.metadata();
    }

    /** Whether the storage may have pages after this read, so that {@link #readNext} is worth calling. */
    protected abstract boolean mayHaveMore(StreamPages last);

    /** Reads the metadata and then the next pages, starting at {@code fromOffset}. Only called when {@link #mayHaveMore} said so. */
    protected abstract void readNext(long fromOffset, ActionListener<StreamPages> listener);

    @Override
    public PageStreamPublisher publisher() {
        return publisher;
    }

    /** The metadata of the latest read from the storage. The last line of the response is written from it. */
    @Override
    public StreamMetadata metadata() {
        return metadata;
    }

    /** Starts feeding the publisher. Nothing is added before the subscriber asks for a page. */
    public void start() {
        StreamPages read;
        synchronized (this) {
            read = first;
            first = null;
        }
        if (read == null) {
            return;
        }
        accept(read);
        publisher.addCloseListener(ActionListener.running(this::close));
        pump();
    }

    /** Takes the pages of a read, trimmed to the rows that may still be added. */
    private void accept(StreamPages read) {
        metadata = read.metadata();
        nextOffset = read.nextOffset();
        long left = rowsLeft;
        boolean release;
        synchronized (this) {
            release = stopped;
            if (release == false) {
                for (Page page : read.pages()) {
                    if (left == 0) {
                        page.releaseBlocks();
                    } else if (page.getPositionCount() > left) {
                        try {
                            ready.add(page.slice(0, Math.toIntExact(left)));
                        } finally {
                            page.releaseBlocks();
                        }
                        left = 0;
                    } else {
                        ready.add(page);
                        left -= page.getPositionCount();
                    }
                }
            }
        }
        if (release) {
            read.close();
            return;
        }
        rowsLeft = left;
        // An empty read is the end of what is stored for now. A GET does not wait for more rows once it has started to answer.
        mayHaveMore = left > 0 && read.pages().isEmpty() == false && mayHaveMore(read);
    }

    /**
     * Adds pages while the publisher is writable, reads more when it has none left, and ends the stream when there is nothing more to
     * read. Only one step runs at a time: each step either returns after it scheduled the next one, or ends the stream.
     */
    private void pump() {
        try {
            while (true) {
                var blocked = publisher.waitForWriting().listener();
                if (blocked.isDone() == false) {
                    blocked.addListener(ActionListener.running(() -> fork(this::pump)));
                    return;
                }
                Page page;
                synchronized (this) {
                    if (stopped) {
                        return;
                    }
                    page = ready.poll();
                }
                if (publisher.isClosed()) {
                    if (page != null) {
                        page.releaseBlocks();
                    }
                    close();
                    return;
                }
                if (page != null) {
                    if (handle.addPage(page) == false) {
                        close();
                        return;
                    }
                } else if (mayHaveMore) {
                    readNext(nextOffset, ActionListener.wrap(read -> {
                        accept(read);
                        fork(this::pump);
                    }, this::fail));
                    return;
                } else {
                    handle.finish();
                    // The subscriber writes the last line from the metadata of this producer, so the footer only marks the end.
                    publisher.completeWithFooter(0, List.of(), false);
                    return;
                }
            }
        } catch (Exception e) {
            fail(e);
        }
    }

    private void fork(Runnable step) {
        try {
            executor.execute(step);
        } catch (Exception e) {
            fail(e);
        }
    }

    /** Ends the stream with a failure. If the response already started, the subscriber reports it in the last line. */
    private void fail(Exception e) {
        close();
        publisher.failStream(e);
    }

    /** Stops the producer and releases the pages it still holds. A read that is running releases its pages when it returns. */
    @Override
    public void close() {
        synchronized (this) {
            stopped = true;
            if (first != null) {
                first.close();
                first = null;
            }
            Page page;
            while ((page = ready.poll()) != null) {
                page.releaseBlocks();
            }
        }
    }
}
