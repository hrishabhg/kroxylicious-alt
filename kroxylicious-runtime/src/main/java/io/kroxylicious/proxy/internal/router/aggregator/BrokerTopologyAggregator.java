/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.router.aggregator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.MetadataResponseData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.router.ResponseAggregationContext;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.service.UpstreamEndpoint;

/**
 * Aggregates MetadataResponse from multiple target clusters into a single unified response.
 * <p>
 * This aggregator performs three key transformations:
 * <ol>
 *   <li><b>Node ID Offsetting</b>: Applies a cluster-specific offset to node IDs to ensure
 *       uniqueness across clusters. For cluster N, node IDs are offset by N * NODE_ID_OFFSET.</li>
 *   <li><b>Address Transformation</b>: Replaces upstream broker addresses with advertised
 *       proxy addresses using the {@link EndpointGateway}'s node identification strategy.</li>
 *   <li><b>Endpoint Reconciliation</b>: Updates the endpoint registry with the discovered
 *       upstream broker topology for connection routing.</li>
 * </ol>
 * </p>
 * <p>
 * The aggregator ensures that:
 * <ul>
 *   <li>Brokers from different clusters have unique node IDs in the merged response</li>
 *   <li>Clients receive proxy addresses they can connect to, not upstream addresses</li>
 *   <li>Duplicate topics (same name across clusters) are handled by keeping the first occurrence</li>
 *   <li>Partition leader/replica/ISR node IDs reference the offset-adjusted node IDs</li>
 * </ul>
 * </p>
 */
public class BrokerTopologyAggregator implements ApiMessageAggregator<MetadataResponseData> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BrokerTopologyAggregator.class);

    private final EndpointGateway endpointGateway;

    /**
     * Offset multiplier for node IDs per cluster. Cluster 0 uses node IDs 0-9999,
     * cluster 1 uses 10000-19999, etc.
     */
    private static final int NODE_ID_OFFSET = 10000;

    public BrokerTopologyAggregator(EndpointGateway endpointGateway) {
        this.endpointGateway = endpointGateway;
    }

    @Override
    public CompletionStage<AggregateResponse<MetadataResponseData>> aggregate(ResponseAggregationContext<MetadataResponseData> context) {
        var aggResponse = new MetadataResponseData();
        var aggBrokers = new MetadataResponseData.MetadataResponseBrokerCollection();
        var aggTopics = new MetadataResponseData.MetadataResponseTopicCollection();
        var seenTopics = new HashSet<String>();

        var upstreamNodeMap = new HashMap<Integer, UpstreamEndpoint>();

        for (var tc : endpointGateway.targetClusters()) {
            int nodeIdOffset = NODE_ID_OFFSET * tc.index();
            var response = context.responses().get(tc);
            if (aggResponse.controllerId() == -1) {
                aggResponse.setControllerId(transformNodeId(response.controllerId(), nodeIdOffset));
                aggResponse.setClusterId(response.clusterId());
            }
            else {
                LOGGER.info("Multiple controller IDs found during aggregation; existing: {}, new: {} from cluster {}",
                        aggResponse.controllerId(),
                        response.controllerId(),
                        tc.name());
            }

            // Aggregate brokers with transformed node IDs and addresses
            for (MetadataResponseData.MetadataResponseBroker originalBroker : response.brokers()) {
                int virtualNodeId = nodeIdOffset + originalBroker.nodeId();

                // Record upstream topology for endpoint reconciliation
                upstreamNodeMap
                        .computeIfAbsent(virtualNodeId, k -> new UpstreamEndpoint(originalBroker.host(), originalBroker.port(), tc, originalBroker.nodeId()));

                // Get the advertised address from the node identification strategy
                HostPort advertisedAddress = endpointGateway.getAdvertisedBrokerAddress(virtualNodeId);

                MetadataResponseData.MetadataResponseBroker newBroker = new MetadataResponseData.MetadataResponseBroker()
                        .setNodeId(virtualNodeId)
                        .setHost(advertisedAddress.host())
                        .setPort(advertisedAddress.port())
                        .setRack(originalBroker.rack());

                boolean added = aggBrokers.add(newBroker);
                if (!added) {
                    LOGGER.warn("Failed to add broker: original_id={}, virtual_id={}",
                            originalBroker.nodeId(), virtualNodeId);
                }

                LOGGER.debug("Transformed broker from cluster {}: nodeId {} -> {}, address {}:{} -> {}",
                        tc.name(), originalBroker.nodeId(), virtualNodeId,
                        originalBroker.host(), originalBroker.port(), advertisedAddress);
            }

            // Aggregate topics with updated partition node IDs
            for (MetadataResponseData.MetadataResponseTopic originalTopic : response.topics()) {
                if (seenTopics.contains(originalTopic.name())) {
                    LOGGER.info("Duplicate topic {} found, ignoring from cluster {}",
                            originalTopic.name(), tc.name());
                    continue;
                }

                // todo: handle if topic is not present in all the clusters
                if (originalTopic.errorCode() != 0) {
                    LOGGER.warn("Skipping topic {} from cluster {} due to error code {}",
                            originalTopic.name(), tc.name(), originalTopic.errorCode());
                    continue;
                }
                seenTopics.add(originalTopic.name());

                MetadataResponseData.MetadataResponseTopic newTopic = new MetadataResponseData.MetadataResponseTopic()
                        .setName(originalTopic.name())
                        .setTopicId(originalTopic.topicId())
                        .setIsInternal(originalTopic.isInternal())
                        .setErrorCode(originalTopic.errorCode())
                        .setTopicAuthorizedOperations(originalTopic.topicAuthorizedOperations());

                var newPartitions = new ArrayList<MetadataResponseData.MetadataResponsePartition>();
                for (MetadataResponseData.MetadataResponsePartition originalPartition : originalTopic.partitions()) {
                    MetadataResponseData.MetadataResponsePartition newPartition = new MetadataResponseData.MetadataResponsePartition()
                            .setPartitionIndex(originalPartition.partitionIndex())
                            .setErrorCode(originalPartition.errorCode())
                            .setLeaderId(transformNodeId(originalPartition.leaderId(), nodeIdOffset))
                            .setLeaderEpoch(originalPartition.leaderEpoch())
                            .setReplicaNodes(originalPartition.replicaNodes().stream()
                                    .map(nodeId -> transformNodeId(nodeId, nodeIdOffset))
                                    .toList())
                            .setIsrNodes(originalPartition.isrNodes().stream()
                                    .map(nodeId -> transformNodeId(nodeId, nodeIdOffset))
                                    .toList())
                            .setOfflineReplicas(originalPartition.offlineReplicas().stream()
                                    .map(nodeId -> transformNodeId(nodeId, nodeIdOffset))
                                    .toList());

                    newPartitions.add(newPartition);
                }
                newTopic.setPartitions(newPartitions);

                boolean topicAdded = aggTopics.add(newTopic);
                LOGGER.debug("Added topic {} from cluster {} with {} partitions ({})",
                        originalTopic.name(), tc.name(), newPartitions.size(), topicAdded);
            }
        }

        aggResponse.setBrokers(aggBrokers);
        aggResponse.setTopics(aggTopics);

        LOGGER.debug("Aggregated {} brokers and {} topics from {} clusters",
                aggBrokers.size(), aggTopics.size(), context.responses().size());

        return context.reconciler().reconcile(endpointGateway, upstreamNodeMap)
                .thenApply(v -> {
                    LOGGER.debug("Endpoint reconciliation completed successfully");
                    return new AggregateResponse<>(context.firstHeader(), aggResponse);
                })
                .whenComplete((v, t) -> {
                    if (t != null) {
                        LOGGER.error("Endpoint reconciliation failed", t);
                        throw new IllegalStateException("Endpoint reconciliation failed", t);
                    }
                });
    }

    /**
     * Transforms a node ID by applying the cluster offset.
     * Preserves sentinel values (negative IDs like -1 indicating no leader).
     *
     * @param nodeId the original node ID
     * @param nodeIdOffset the offset to apply for the cluster
     * @return the transformed node ID, or the original if negative
     */
    private static int transformNodeId(int nodeId, int nodeIdOffset) {
        return nodeIdOffset + nodeId;
    }
}
