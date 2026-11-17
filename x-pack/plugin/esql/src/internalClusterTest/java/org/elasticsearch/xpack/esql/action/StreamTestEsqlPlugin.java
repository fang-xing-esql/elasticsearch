/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.plugin.TransportEsqlStreamGetResultsAction;
import org.elasticsearch.xpack.esql.streaming.InMemoryStreamStore;
import org.elasticsearch.xpack.esql.streaming.StreamStorage;

import java.util.ArrayList;
import java.util.List;

/**
 * The ES|QL plugin with a stream GET that reads from an {@link InMemoryStreamStore}, until the real storage exists. The storage is
 * created by the transport action, so the test replaces that action, the way a plugin subclass already replaces the license state.
 * <p>
 * The storage is shared by the nodes of the test cluster, which has a single node, and streams are deleted by the tests.
 */
public class StreamTestEsqlPlugin extends EsqlPluginWithEnterpriseOrTrialLicense {

    /** The storage the stream GET reads from. */
    public static final InMemoryStreamStore STORE = new InMemoryStreamStore();

    @Override
    public List<ActionHandler> getActions() {
        List<ActionHandler> actions = new ArrayList<>();
        for (ActionHandler handler : super.getActions()) {
            if (handler.getAction() == EsqlStreamGetResultAction.INSTANCE) {
                actions.add(new ActionHandler(EsqlStreamGetResultAction.INSTANCE, InMemoryTransportEsqlStreamGetResultsAction.class));
            } else {
                actions.add(handler);
            }
        }
        return actions;
    }

    /** The stream GET action, reading from {@link #STORE}. */
    public static class InMemoryTransportEsqlStreamGetResultsAction extends TransportEsqlStreamGetResultsAction {
        @Inject
        public InMemoryTransportEsqlStreamGetResultsAction(
            TransportService transportService,
            ActionFilters actionFilters,
            ClusterService clusterService,
            ThreadPool threadPool,
            Client client,
            BigArrays bigArrays,
            BlockFactoryProvider blockFactoryProvider
        ) {
            super(transportService, actionFilters, clusterService, threadPool, client, bigArrays, blockFactoryProvider);
        }

        @Override
        protected StreamStorage createStreamStorage(ClusterService clusterService, ThreadPool threadPool, Client client) {
            return STORE;
        }
    }
}
