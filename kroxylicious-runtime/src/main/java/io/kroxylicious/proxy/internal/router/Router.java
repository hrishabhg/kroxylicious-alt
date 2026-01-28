/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.router;

import java.util.List;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.internal.net.BootstrapEndpointBinding;
import io.kroxylicious.proxy.internal.net.BrokerEndpointBinding;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.net.MetadataDiscoveryBrokerEndpointBinding;
import io.kroxylicious.proxy.internal.router.aggregator.ApiMessageAggregator;
import io.kroxylicious.proxy.service.UpstreamEndpoint;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Router decides which cluster(s) to route requests to.
 *
 * <p>Router is stateless configuration, created at bootstrap time.
 * It makes routing decisions based on:</p>
 * <ul>
 *   <li>Request ApiKey (Metadata, Produce, Fetch, etc.)</li>
 *   <li>Request content (topic name, consumer group, etc.)</li>
 *   <li>Configured routing rules</li>
 * </ul>
 */
public interface Router {

    BootstrapEndpointBinding bootstrapEndpointBinding(EndpointGateway endpointGateway);

    /**
     * Get broker endpoint binding for given nodeId and target cluster.
     * @param endpointGateway gateway
     * @param virtualNodeId nodeId
     * @param upstreamNodeId upstreamNodeId in target cluster
     * @param hostPort hostPort of the broker
     * @param targetCluster associated target cluster
     * @return broker endpoint binding
     */
    BrokerEndpointBinding brokerEndpointBinding(EndpointGateway endpointGateway, int virtualNodeId, UpstreamEndpoint upstreamEndpoint);

    MetadataDiscoveryBrokerEndpointBinding metadataDiscoveryBrokerEndpointBinding(EndpointGateway endpointGateway, int virtualNodeId);

    default MetadataDiscoveryBrokerEndpointBinding metadataDiscoveryBrokerEndpointBinding(BootstrapEndpointBinding bootstrapEndpointBinding, int virtualNodeId) {
        return new MetadataDiscoveryBrokerEndpointBinding() {
            @NonNull
            @Override
            public Integer upstreamNodeId() {
                return null;
            }

            @NonNull
            @Override
            public EndpointGateway endpointGateway() {
                return bootstrapEndpointBinding.endpointGateway();
            }

            @NonNull
            @Override
            public List<UpstreamEndpoint> upstreamServiceEndpoints(ApiKeys apiKey) {
                return bootstrapEndpointBinding.upstreamServiceEndpoints(apiKey);
            }

            @NonNull
            @Override
            public List<UpstreamEndpoint> allUpstreamServiceEndpoints() {
                return bootstrapEndpointBinding.allUpstreamServiceEndpoints();
            }

            @NonNull
            @Override
            public Integer virtualNodeId() {
                // For discovery bindings created from bootstrap, we don't have cluster context
                // so virtualNodeId equals upstreamNodeId (assuming cluster 0)
                return virtualNodeId;
            }
        };
    }

    /**
     * Get response aggregator factory for an ApiKey.
     * Returns default if this ApiKey doesn't require aggregation.
     */
    <T extends ApiMessage> ApiMessageAggregator<T> aggregator(ApiKeys apiKey);
}
