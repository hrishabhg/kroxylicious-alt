/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.internal.codec.CorrelationManager;
import io.kroxylicious.proxy.internal.codec.KafkaMessageListener;
import io.kroxylicious.proxy.internal.codec.KafkaRequestEncoder;
import io.kroxylicious.proxy.internal.codec.KafkaResponseDecoder;
import io.kroxylicious.proxy.internal.metrics.MetricEmittingKafkaMessageListener;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.service.UpstreamEndpoint;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Manages the state machine for a single backend cluster connection.
 *
 * <p>Each cluster in a multi-cluster setup has its own BackendStateMachine,
 * allowing independent connection lifecycle, backpressure, and failure handling.</p>
 *
 * <p>This class encapsulates:</p>
 * <ul>
 *   <li>Connection state (Created → Connecting → Connected → Closed)</li>
 *   <li>The Netty channel and pipeline for this cluster</li>
 *   <li>Backpressure state for this specific connection</li>
 *   <li>Request forwarding to this cluster</li>
 * </ul>
 *
 * <p>Works with {@link KafkaProxyBackendHandler} for Netty I/O operations.</p>
 */
public class BackendStateMachine {

    private static final Logger LOGGER = LoggerFactory.getLogger(BackendStateMachine.class);

    private final String clusterId;
    private final TargetCluster targetCluster;
    private final UpstreamEndpoint upstreamEndpoint;
    private final ProxyChannelStateMachine proxyChannelStateMachine;
    private final int socketFrameMaxSizeBytes;
    private final boolean logNetwork;
    private final boolean logFrames;

    // Mutable state
    private volatile BackendConnectionState state = BackendConnectionState.Created.INSTANCE;
    private @Nullable KafkaProxyBackendHandler backendHandler;
    private @Nullable Channel channel;

    // Backpressure tracking
    private volatile boolean readsBlocked = false;
    private @Nullable Timer.Sample backpressureTimer;

    // Connection promise - completed when connection is ready
    private final CompletableFuture<BackendStateMachine> connectionFuture = new CompletableFuture<>();

    // Metrics
    private final Counter connectionCounter;
    private final Counter errorCounter;
    private final Timer backpressureTimer_;

    public BackendStateMachine(
                               UpstreamEndpoint upstreamEndpoint,
                               ProxyChannelStateMachine proxyChannelStateMachine,
                               int socketFrameMaxSizeBytes,
                               boolean logNetwork,
                               boolean logFrames,
                               Counter connectionCounter,
                               Counter errorCounter,
                               Timer backpressureTimer) {
        this.clusterId = Objects.requireNonNull(upstreamEndpoint.targetCluster().name());
        this.targetCluster = Objects.requireNonNull(upstreamEndpoint.targetCluster());
        this.proxyChannelStateMachine = Objects.requireNonNull(proxyChannelStateMachine);
        this.socketFrameMaxSizeBytes = socketFrameMaxSizeBytes;
        this.logNetwork = logNetwork;
        this.logFrames = logFrames;
        this.connectionCounter = connectionCounter;
        this.errorCounter = errorCounter;
        this.backpressureTimer_ = backpressureTimer;
        this.upstreamEndpoint = Objects.requireNonNull(upstreamEndpoint);
    }

    // ==================== Accessors ====================

    public String clusterId() {
        return clusterId;
    }

    public TargetCluster targetCluster() {
        return targetCluster;
    }

    public UpstreamEndpoint endpoint() {
        return upstreamEndpoint;
    }

    public BackendConnectionState state() {
        return state;
    }

    public boolean isConnected() {
        return state.isConnected();
    }

    public CompletableFuture<BackendStateMachine> connectionFuture() {
        return connectionFuture;
    }

    /**
     * Get the session ID from the connection manager.
     */
    public String sessionId() {
        return proxyChannelStateMachine.sessionId();
    }

    /**
     * Get the Netty handler for this backend connection.
     */
    @Nullable
    public KafkaProxyBackendHandler backendHandler() {
        return backendHandler;
    }

    // ==================== Connection Lifecycle ====================

    /**
     * Initiate connection to this cluster.
     *
     * @param inboundChannel the client channel (used for event loop)
     * @return future that completes when connection is established
     */
    public CompletableFuture<BackendStateMachine> connect(Channel inboundChannel) {

        if (!(state instanceof BackendConnectionState.Created)) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Cannot connect from state: " + state));
        }

        HostPort target = upstreamEndpoint.getHostPort();
        LOGGER.debug("{}: Connecting to cluster {} at {}",
                proxyChannelStateMachine.sessionId(), clusterId, target);

        // Transition to Connecting
        Optional<SslContext> sslContext = VirtualClusterModel.buildUpstreamSslContext(targetCluster.tls());
        BackendConnectionState.Connecting connecting = ((BackendConnectionState.Created) state).toConnecting(upstreamEndpoint);
        setState(connecting);

        // Create backend handler - uses the existing KafkaProxyBackendHandler
        this.backendHandler = new KafkaProxyBackendHandler(this, sslContext);

        // Configure and connect
        Bootstrap bootstrap = configureBootstrap(inboundChannel);
        ChannelFuture connectFuture = bootstrap.connect(target.host(), target.port());
        this.channel = connectFuture.channel();

        // Setup pipeline
        configurePipeline(channel.pipeline(), sslContext, target);

        // Handle connection result
        connectFuture.addListener((ChannelFuture f) -> {
            if (f.isSuccess()) {
                LOGGER.trace("{}: TCP connected to {} for cluster {}",
                        proxyChannelStateMachine.sessionId(), target, clusterId);
                // For non-TLS, onConnectionActive() will be called from channelActive() in the handler
                // For TLS, it will be called from userEventTriggered() after SslHandshakeCompletionEvent
            }
            else {
                onConnectionFailed(f.cause());
            }
        });

        return connectionFuture;
    }

    /**
     * Called when the connection becomes active (TCP + optional TLS handshake complete).
     * Called from KafkaProxyBackendHandler.
     */
    public void onConnectionActive() {
        if (state instanceof BackendConnectionState.Connecting connecting) {
            BackendConnectionState.Connected connected = connecting.toConnected();
            setState(connected);
            connectionCounter.increment();

            LOGGER.info("{}: Connected to cluster {} at {}",
                    proxyChannelStateMachine.sessionId(), upstreamEndpoint.targetCluster().name(), connecting.endpoint());

            connectionFuture.complete(this);
            proxyChannelStateMachine.onBackendConnected(this);
        }
        else {
            LOGGER.warn("{}: Unexpected onConnectionActive in state {}",
                    proxyChannelStateMachine.sessionId(), state);
        }
    }

    /**
     * Called when connection attempt fails.
     * Called from KafkaProxyBackendHandler.
     */
    public void onConnectionFailed(Throwable cause) {
        if (state instanceof BackendConnectionState.Connecting connecting) {
            BackendConnectionState.Failed failed = connecting.toFailed(cause);
            setState(failed);
            errorCounter.increment();

            LOGGER.warn("{}: Failed to connect to cluster {} at {}: {}",
                    proxyChannelStateMachine.sessionId(), clusterId, connecting.endpoint(), cause.getMessage());

            connectionFuture.completeExceptionally(cause);
            proxyChannelStateMachine.onBackendFailed(this, cause);
        }
    }

    /**
     * Called when an established connection becomes inactive.
     * Called from KafkaProxyBackendHandler.
     */
    public void onConnectionInactive() {
        if (state instanceof BackendConnectionState.Connected connected) {
            setState(connected.toClosed());
            LOGGER.debug("{}: Connection to cluster {} closed",
                    proxyChannelStateMachine.sessionId(), clusterId);
            proxyChannelStateMachine.onBackendClosed(this);
        }
    }

    /**
     * Called when an error occurs on the connection.
     * Called from KafkaProxyBackendHandler.
     */
    public void onConnectionError(Throwable cause) {
        LOGGER.error("{}: Error on cluster {} connection: {}",
                proxyChannelStateMachine.sessionId(), clusterId, cause.getMessage());
        errorCounter.increment();

        if (state instanceof BackendConnectionState.Connected connected) {
            setState(connected.toClosed());
            proxyChannelStateMachine.onBackendError(this, cause);
        }
        else if (state instanceof BackendConnectionState.Connecting) {
            onConnectionFailed(cause);
        }
    }

    /**
     * Close this backend connection.
     */
    public void close() {
        if (state.isTerminal()) {
            return;
        }

        LOGGER.debug("{}: Closing connection to cluster {}",
                proxyChannelStateMachine.sessionId(), clusterId);

        if (state instanceof BackendConnectionState.Connected connected) {
            setState(connected.toClosed());
        }
        else if (state instanceof BackendConnectionState.Connecting) {
            setState(BackendConnectionState.Closed.INSTANCE);
        }

        if (backendHandler != null) {
            backendHandler.inClosed();
        }
        else if (channel != null && channel.isActive()) {
            channel.writeAndFlush(Unpooled.EMPTY_BUFFER)
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

    // ==================== Message Handling (from KafkaProxyBackendHandler) ====================

    /**
     * Called when a message is received from the server.
     * Called from KafkaProxyBackendHandler.channelRead().
     */
    public void onMessageFromServer(Object msg) {
        proxyChannelStateMachine.onBackendResponse(this, msg);
    }

    /**
     * Called when server read batch is complete.
     * Called from KafkaProxyBackendHandler.channelReadComplete().
     */
    public void onServerReadComplete() {
        proxyChannelStateMachine.onBackendReadComplete(this);
    }

    /**
     * Called when server becomes writable (backpressure relieved).
     * Called from KafkaProxyBackendHandler.channelWritabilityChanged().
     */
    public void onServerWritable() {
        proxyChannelStateMachine.onBackendWritable(this);
    }

    /**
     * Called when server becomes unwritable (backpressure applied).
     * Called from KafkaProxyBackendHandler.channelWritabilityChanged().
     */
    public void onServerUnwritable() {
        proxyChannelStateMachine.onBackendUnwritable(this);
    }

    // ==================== Request Forwarding ====================

    /**
     * Forward a message to this cluster's backend.
     */
    public void forwardToServer(Object msg) {
        if (!state.canSendRequests()) {
            throw new IllegalStateException(
                    "Cannot forward to cluster " + clusterId + " in state: " + state);
        }

        if (backendHandler == null) {
            throw new IllegalStateException("Backend handler not available for cluster " + clusterId);
        }

        backendHandler.forwardToServer(msg);
    }

    /**
     * Flush pending writes to this cluster.
     */
    public void flushToServer() {
        if (backendHandler != null) {
            backendHandler.flushToServer();
        }
    }

    // ==================== Backpressure ====================

    /**
     * Apply backpressure - stop reading from this backend.
     */
    public void applyBackpressure() {
        if (!readsBlocked && backendHandler != null) {
            readsBlocked = true;
            backpressureTimer = Timer.start();
            backendHandler.applyBackpressure();
            LOGGER.trace("{}: Backpressure applied to cluster {}",
                    proxyChannelStateMachine.sessionId(), clusterId);
        }
    }

    /**
     * Relieve backpressure - resume reading from this backend.
     */
    public void relieveBackpressure() {
        if (readsBlocked && backendHandler != null) {
            readsBlocked = false;
            if (backpressureTimer != null) {
                backpressureTimer.stop(backpressureTimer_);
                backpressureTimer = null;
            }
            backendHandler.relieveBackpressure();
            LOGGER.trace("{}: Backpressure relieved from cluster {}",
                    proxyChannelStateMachine.sessionId(), clusterId);
        }
    }

    public boolean isReadsBlocked() {
        return readsBlocked;
    }

    /**
     * Check if this backend's channel is writable (can accept more data).
     *
     * @return true if the channel is writable, false otherwise
     */
    public boolean isChannelWritable() {
        return backendHandler != null && backendHandler.isChannelWritable();
    }

    // ==================== Pipeline Configuration ====================

    @VisibleForTesting
    Bootstrap configureBootstrap(Channel inboundChannel) {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(inboundChannel.eventLoop())
                .channel(inboundChannel.getClass())
                .handler(backendHandler)
                .option(ChannelOption.AUTO_READ, true)
                .option(ChannelOption.TCP_NODELAY, true);
        return bootstrap;
    }

    private void configurePipeline(
                                   ChannelPipeline pipeline,
                                   Optional<SslContext> sslContext,
                                   HostPort target) {

        CorrelationManager correlationManager = new CorrelationManager();

        if (logFrames) {
            pipeline.addFirst("frameLogger",
                    new LoggingHandler("io.kroxylicious.proxy.internal.UpstreamFrameLogger." + targetCluster().name(),
                            LogLevel.INFO));
        }

        var encoderListener = buildMetricsMessageListenerForEncode();
        var decoderListener = buildMetricsMessageListenerForDecode();

        // todo: check if null needs to be replaced with a listener
        pipeline.addFirst("responseDecoder",
                new KafkaResponseDecoder(correlationManager, socketFrameMaxSizeBytes, decoderListener));
        pipeline.addFirst("requestEncoder",
                new KafkaRequestEncoder(correlationManager, encoderListener));

        if (logNetwork) {
            pipeline.addFirst("networkLogger",
                    new LoggingHandler("io.kroxylicious.proxy.internal.UpstreamNetworkLogger." + clusterId,
                            LogLevel.INFO));
        }

        sslContext.ifPresent(ssl -> {
            SslHandler handler = ssl.newHandler(pipeline.channel().alloc(), target.host(), target.port());
            pipeline.addFirst("ssl", handler);
        });

        LOGGER.debug("{}: Configured pipeline for cluster {}: {}",
                proxyChannelStateMachine.sessionId(), clusterId, pipeline);
    }

    // ==================== Internal State Management ====================

    private void setState(BackendConnectionState newState) {
        LOGGER.trace("{}: Cluster {} state {} -> {}",
                proxyChannelStateMachine.sessionId(), clusterId, state, newState);
        this.state = newState;
    }

    /**
     * Report an illegal state condition.
     */
    public void illegalState(String msg) {
        LOGGER.error("{}: Cluster {} illegal state: {}", proxyChannelStateMachine.sessionId(), clusterId, msg);
        close();
    }

    private MetricEmittingKafkaMessageListener buildMetricsMessageListenerForEncode() {
        var clusterName = proxyChannelStateMachine.virtualClusterName();
        var nodeId = proxyChannelStateMachine.nodeId();
        var proxyToServerMessageCounterProvider = Metrics.proxyToServerMessageCounterProvider(clusterName, nodeId);
        var proxyToServerMessageSizeDistributionProvider = Metrics.proxyToServerMessageSizeDistributionProvider(clusterName,
                nodeId);
        return new MetricEmittingKafkaMessageListener(proxyToServerMessageCounterProvider, proxyToServerMessageSizeDistributionProvider);
    }

    private KafkaMessageListener buildMetricsMessageListenerForDecode() {
        var clusterName = proxyChannelStateMachine.virtualClusterName();
        var nodeId = proxyChannelStateMachine.nodeId();
        var serverToProxyMessageCounterProvider = Metrics.serverToProxyMessageCounterProvider(clusterName, nodeId);

        var serverToProxyMessageSizeDistributionProvider = Metrics.serverToProxyMessageSizeDistributionProvider(clusterName,
                nodeId);
        return new MetricEmittingKafkaMessageListener(serverToProxyMessageCounterProvider, serverToProxyMessageSizeDistributionProvider);
    }

    @Override
    public String toString() {
        return "BackendStateMachine{" +
                "clusterId='" + clusterId + '\'' +
                ", state=" + state +
                ", readsBlocked=" + readsBlocked +
                '}';
    }
}
