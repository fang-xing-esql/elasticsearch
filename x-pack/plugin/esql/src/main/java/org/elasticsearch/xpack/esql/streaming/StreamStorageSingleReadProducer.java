/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.action.ActionListener;

import java.util.concurrent.Executor;

/**
 * The producer of the {@link GetStreamResultRequest.ReadMode#PAGES} mode: every page comes from the one {@code getPages} call that
 * was made before the response started. It holds all of them and hands them to the publisher one at a time, as the client asks.
 * It is also the producer of a status-only request, which has no pages.
 */
public final class StreamStorageSingleReadProducer extends StreamPageProducer {

    public StreamStorageSingleReadProducer(Executor executor, StreamPages first, long maxRows) {
        super(executor, first, maxRows);
    }

    @Override
    protected boolean mayHaveMore(StreamPages last) {
        return false;
    }

    @Override
    protected void readNext(long fromOffset, ActionListener<StreamPages> listener) {
        assert false : "the single read has no next read";
        listener.onFailure(new IllegalStateException("the single read has no next read"));
    }
}
