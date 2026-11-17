/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;

import java.util.concurrent.Flow;

/**
 * The pages of a stream read, as the response body sees them, whatever the read mode of the request: a publisher that delivers one
 * page for each unit of demand, and the metadata of the latest read from the storage, for the last line of the response.
 * <p>
 * Closing it stops whatever feeds the publisher and releases the pages that were not delivered.
 */
public interface StreamResultPages extends Releasable {

    /** The pages of the result. It completes when there is nothing more to return, and fails if a read from the storage fails. */
    Flow.Publisher<Page> publisher();

    /** The metadata of the latest read from the storage. */
    StreamMetadata metadata();
}
