/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.service;

import java.util.Objects;

import io.kroxylicious.proxy.config.TargetCluster;

import edu.umd.cs.findbugs.annotations.Nullable;

// aka HostPort with Tls
public record UpstreamEndpoint(String host, int port, TargetCluster targetCluster, @Nullable Integer upstreamNodeId) {

    public UpstreamEndpoint {
        Objects.requireNonNull(host, "host cannot be null");
        Objects.requireNonNull(targetCluster, "targetCluster cannot be null");
    }

    public HostPort getHostPort() {
        return new HostPort(host, port);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, targetCluster, upstreamNodeId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (UpstreamEndpoint) obj;
        return this.host.equalsIgnoreCase(that.host)
                && this.port == that.port
                && this.targetCluster.equals(that.targetCluster)
                && ((this.upstreamNodeId == null && that.upstreamNodeId == null)
                        || (this.upstreamNodeId != null && this.upstreamNodeId.equals(that.upstreamNodeId)));
    }

    @Override
    public String toString() {
        return "UpstreamEndpoint{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", targetCluster=" + targetCluster.name() +
                ", upstreamNodeId=" + upstreamNodeId +
                '}';
    }
}