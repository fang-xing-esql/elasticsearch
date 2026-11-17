/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasable;

import java.util.List;

/**
 * The result of one read from the storage: the metadata as it was just before the pages were read, and the pages that cover the requested
 * range. Whoever holds it owns its pages and must close it, or hand the pages on and stop using it.
 *
 * @param metadata   the metadata read <b>before</b> the pages. If its status is terminal, an empty read really is the end of the result.
 * @param pages      shallow copies of the stored pages, trimmed to start at the requested offset. They are owned by the holder.
 * @param nextOffset the row where the next read from the storage starts, to continue after these pages. A read of a row range ends
 *                   where its pages end. A read of whole pages may end after the rows that the request wants.
 */
public record StreamPages(StreamMetadata metadata, List<Page> pages, long nextOffset) implements Releasable {

    public long rows() {
        long rows = 0;
        for (Page page : pages) {
            rows += page.getPositionCount();
        }
        return rows;
    }

    public boolean isTerminal() {
        return metadata.status() != StreamMetadata.Status.RUNNING;
    }

    @Override
    public void close() {
        for (Page page : pages) {
            page.releaseBlocks();
        }
    }
}
