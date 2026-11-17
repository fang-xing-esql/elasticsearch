/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.PageStreamPublisher;
import org.elasticsearch.compute.operator.StreamingPageOperator;
import org.elasticsearch.tasks.CancellableTask;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.function.Function;

/**
 * Runs a stream GET as a driver, the {@link GetStreamResultRequest.ReadMode#DRIVER} mode: a {@link StreamStorageSourceOperator}
 * reads the storage, and a {@link StreamingPageOperator}, the sink of the streaming POST, feeds the publisher that the response body
 * subscribes to. The driver does what {@link StreamPageProducer} does by hand in the other modes: it waits until the client is ready
 * for a page, moves one, and stops when the source is finished, the client leaves or a read fails.
 */
public final class StreamStorageDriver implements StreamResultPages {

    /**
     * What a driver needs from the node.
     *
     * @param executor runs the driver. It is the pool of the ES|QL drivers in production.
     */
    public record Settings(BigArrays bigArrays, BlockFactory blockFactory, Executor executor, String clusterName, String nodeName) {}

    private final PageStreamPublisher publisher = new PageStreamPublisher();
    private final StreamStorageSourceOperator source;
    private final Driver driver;

    private StreamStorageDriver(
        Settings settings,
        StreamStorage store,
        GetStreamResultRequest request,
        StreamPages first,
        int batchPages,
        long batchBytes
    ) {
        DriverContext driverContext = new DriverContext(settings.bigArrays(), settings.blockFactory(), null);
        this.source = new StreamStorageSourceOperator(
            driverContext,
            store,
            request.id(),
            request.offset(),
            first,
            request.maxRows(),
            batchPages,
            batchBytes
        );
        this.driver = new Driver(
            "stream-get:" + request.id(),
            "stream-get",
            settings.clusterName(),
            settings.nodeName(),
            System.currentTimeMillis(),
            System.nanoTime(),
            driverContext,
            () -> "stream get [" + request.id() + "]",
            source,
            List.of(),
            // The stored pages already have the columns of the stream, so there is nothing to align.
            new StreamingPageOperator(publisher, Function.identity()),
            Driver.DEFAULT_STATUS_INTERVAL,
            () -> {}
        );
    }

    /**
     * Creates the driver and starts it. It moves no page before the subscriber of {@link #publisher()} asks for one.
     *
     * @param task  the task of the GET, or {@code null}. Cancelling it cancels the driver.
     * @param first the result of the first read, made before the response started. The driver takes its pages.
     */
    public static StreamStorageDriver start(
        Settings settings,
        ThreadContext threadContext,
        CancellableTask task,
        StreamStorage store,
        GetStreamResultRequest request,
        StreamPages first,
        int batchPages,
        long batchBytes
    ) {
        StreamStorageDriver pages = new StreamStorageDriver(settings, store, request, first, batchPages, batchBytes);
        if (task != null) {
            task.addListener(() -> pages.driver.cancel(Objects.requireNonNullElse(task.getReasonCancelled(), "task was cancelled")));
        }
        PageStreamPublisher publisher = pages.publisher;
        Driver.start(
            threadContext,
            settings.executor(),
            pages.driver,
            Driver.DEFAULT_MAX_ITERATIONS,
            // The subscriber writes the last line from the metadata of the source, so the footer only marks the end. A failure after
            // the response started is reported in the last line.
            ActionListener.wrap(ignored -> publisher.completeWithFooter(0, List.of(), false), publisher::failStream)
        );
        return pages;
    }

    @Override
    public Flow.Publisher<Page> publisher() {
        return publisher;
    }

    @Override
    public StreamMetadata metadata() {
        return source.metadata();
    }

    /** Cancels the driver, which closes its operators and releases the pages they hold. It does nothing if the driver is done. */
    @Override
    public void close() {
        driver.cancel("the stream get response was closed");
    }
}
