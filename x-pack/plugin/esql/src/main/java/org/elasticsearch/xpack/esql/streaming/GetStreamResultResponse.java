/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.util.concurrent.Flow;

/**
 * The result of a stream read: the metadata of the first read and the producer that feeds the pages to whoever subscribes to
 * {@link #publisher()}. It lives on one node and is never serialized.
 * <p>
 * The pages are not all here when the response is created. The subscriber asks for them one at a time, and depending on the read mode
 * of the request the producer already holds them or reads them from the storage as they are asked for.
 * <p>
 * Closing the response stops the producer and releases the pages that were not delivered.
 */
public class GetStreamResultResponse extends ActionResponse implements Releasable {

    private final GetStreamResultRequest request;
    private final StreamMetadata metadata;
    private final StreamResultPages producer;

    /**
     * @param request  the request this answers, for its offset and limits
     * @param metadata the metadata read before the first pages. The first line of the response is written from it.
     * @param producer the pages, already started: a producer or a driver, depending on the read mode. For a status-only request
     *                 it has no page.
     */
    public GetStreamResultResponse(GetStreamResultRequest request, StreamMetadata metadata, StreamResultPages producer) {
        this.request = request;
        this.metadata = metadata;
        this.producer = producer;
    }

    public GetStreamResultRequest request() {
        return request;
    }

    /** The metadata read before the first pages. */
    public StreamMetadata metadata() {
        return metadata;
    }

    /**
     * The metadata of the latest read from the storage. While the response is written it can be newer than {@link #metadata()}. The
     * last line of the response is written from it.
     */
    public StreamMetadata latestMetadata() {
        return producer.metadata();
    }

    /** The pages of the result, one for each unit of demand. It completes when there is nothing more to return. */
    public Flow.Publisher<Page> publisher() {
        return producer.publisher();
    }

    @Override
    public void close() {
        producer.close();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TransportAction.localOnly();
    }
}
