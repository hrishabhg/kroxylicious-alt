/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

public interface MetadataDiscoveryBrokerEndpointBinding extends NodeSpecificEndpointBinding {
    @Override
    default boolean restrictUpstreamToMetadataDiscovery() {
        return true;
    }
}
