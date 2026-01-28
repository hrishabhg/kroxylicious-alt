/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import io.kroxylicious.proxy.service.UpstreamEndpoint;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Sealed hierarchy representing backend connection states.
 * Each cluster connection has its own independent state machine.
 *
 * <pre>
 *   Created
 *      │
 *      ▼ connect()
 *   Connecting
 *      │
 *      ├──────────────► Failed (connection error)
 *      │
 *      ▼ onActive()
 *   Connected
 *      │
 *      ▼ onInactive() or onError()
 *   Closed
 * </pre>
 */
public sealed interface BackendConnectionState permits
        BackendConnectionState.Created,
        BackendConnectionState.Connecting,
        BackendConnectionState.Connected,
        BackendConnectionState.Failed,
        BackendConnectionState.Closed {

    /**
     * Initial state - backend not yet connecting.
     */
    record Created() implements BackendConnectionState {
        public static final Created INSTANCE = new Created();

        public Connecting toConnecting(UpstreamEndpoint endpoint) {
            return new Connecting(endpoint);
        }
    }

    /**
     * TCP connection in progress (and TLS handshake if applicable).
     */
    record Connecting(UpstreamEndpoint endpoint)
            implements BackendConnectionState {

        public Connected toConnected() {
            return new Connected(endpoint);
        }

        public Failed toFailed(Throwable cause) {
            return new Failed(endpoint, cause);
        }
    }

    /**
     * Connection established and ready for KRPC.
     */
    record Connected(UpstreamEndpoint endpoint) implements BackendConnectionState {

        public Closed toClosed() {
            return Closed.INSTANCE;
        }
    }

    /**
     * Connection attempt failed.
     */
    record Failed(
                  UpstreamEndpoint endpoint,
                  @Nullable Throwable cause)
            implements BackendConnectionState {

        public Closed toClosed() {
            return Closed.INSTANCE;
        }
    }

    /**
     * Terminal state - connection closed.
     */
    record Closed() implements BackendConnectionState { public static final Closed INSTANCE = new Closed(); }

    // Convenience methods
    default boolean isConnected() {
        return this instanceof Connected;
    }

    default boolean isTerminal() {
        return this instanceof Closed || this instanceof Failed;
    }

    default boolean canSendRequests() {
        return this instanceof Connected;
    }
}