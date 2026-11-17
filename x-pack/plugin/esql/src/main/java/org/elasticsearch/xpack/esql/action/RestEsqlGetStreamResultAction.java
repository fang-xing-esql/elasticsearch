/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.xpack.esql.streaming.GetStreamResultRequest;
import org.elasticsearch.xpack.esql.streaming.GetStreamResultResponse;

import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.GET;

/**
 * {@code GET /_query/stream/{id}}: reads the stored result of a stream as NDJSON, from {@code offset}, limited by {@code max_rows} and
 * {@code max_bytes} when they are set. It is the counterpart of {@code RestEsqlGetAsyncResultAction}, and {@code wait_timeout} plays the
 * role of {@code wait_for_completion_timeout}.
 */
public class RestEsqlGetStreamResultAction extends BaseRestHandler {

    static final String READ_MODE_PARAM = "read_mode";

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_query/stream/{id}"));
    }

    @Override
    public String getName() {
        return "esql_get_stream_result";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        GetStreamResultRequest get = parseRequest(request);
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).execute(
            EsqlStreamGetResultAction.INSTANCE,
            get,
            new EsqlStreamResultListener(channel)
        );
    }

    /**
     * Reads the parameters. A limit that is not provided stays unlimited, and an unset {@code wait_timeout} means no wait, as an unset
     * {@code wait_for_completion_timeout} does for the async API. A value that cannot be parsed or is negative is a 400.
     * <p>
     * {@code read_mode} ({@code pages}, {@code batch} or {@code driver}) chooses how the pages are read from the storage. It is there
     * to benchmark the ways against each other, it is not part of the API, and like a pragma it is only accepted in snapshot builds.
     */
    static GetStreamResultRequest parseRequest(RestRequest request) {
        String id = request.param("id");
        long offset = request.paramAsLong("offset", 0L);
        long maxRows = GetStreamResultRequest.NO_LIMIT;
        if (request.hasParam("max_rows")) {
            maxRows = request.paramAsLong("max_rows", 0L);
            if (maxRows < 0) {
                // a negative value must not be read as the internal "not provided" marker
                throw new IllegalArgumentException("[max_rows] must be greater than or equal to 0 but was [" + maxRows + "]");
            }
        }
        long maxBytes = GetStreamResultRequest.NO_LIMIT;
        if (request.hasParam("max_bytes")) {
            maxBytes = request.paramAsSize("max_bytes", null).getBytes();
            if (maxBytes < 0) {
                throw new IllegalArgumentException("[max_bytes] must be greater than or equal to 0 but was [" + maxBytes + "]");
            }
        }
        TimeValue waitTimeout = request.hasParam("wait_timeout") ? request.paramAsTime("wait_timeout", TimeValue.MINUS_ONE) : null;
        GetStreamResultRequest.ReadMode readMode = GetStreamResultRequest.ReadMode.DEFAULT;
        if (request.hasParam(READ_MODE_PARAM)) {
            String value = request.param(READ_MODE_PARAM);
            if (Build.current().isSnapshot() == false) {
                throw new IllegalArgumentException("[" + READ_MODE_PARAM + "] only allowed in snapshot builds");
            }
            readMode = GetStreamResultRequest.ReadMode.fromString(value);
        }
        return new GetStreamResultRequest(id, offset, maxRows, maxBytes, waitTimeout, readMode);
    }

    /** Sends the NDJSON body, or the error when the read failed before the first line. */
    private static final class EsqlStreamResultListener extends RestActionListener<GetStreamResultResponse> {
        private final RestChannel restChannel;

        EsqlStreamResultListener(RestChannel channel) {
            super(channel);
            this.restChannel = channel;
        }

        @Override
        protected void processResponse(GetStreamResultResponse response) throws Exception {
            StreamNdjsonBody body;
            try {
                body = new StreamNdjsonBody(restChannel, response);
            } catch (Exception e) {
                response.close();
                throw e;
            }
            boolean sent = false;
            try {
                restChannel.sendResponse(RestResponse.chunked(RestStatus.OK, body.firstPart(), body));
                sent = true;
            } finally {
                if (sent == false) {
                    body.close();
                }
            }
        }
    }
}
