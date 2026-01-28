/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.router.aggregator;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.TargetCluster;
import io.kroxylicious.proxy.internal.ApiVersionsServiceImpl;
import io.kroxylicious.proxy.internal.router.ResponseAggregationContext;

/**
 * Aggregates ApiVersions responses from multiple target clusters.
 *
 * Logic:
 * - Negotiates supported API versions across all clusters:
 *   - Min version = MAX of all cluster minimums (most restrictive)
 *   - Max version = MIN of all cluster maximums (most restrictive)
 * - If an API is not supported by a cluster, versions are set to 0
 * - If any cluster reports an error, intersection is still performed but
 *   the error code from the cluster with the fewest APIs is returned
 *
 * This ensures the proxy only advertises API versions supported by ALL clusters.
 */
public class ApiVersionResponseAggregator implements ApiMessageAggregator<ApiVersionsResponseData> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiVersionResponseAggregator.class);
    private final ApiVersionsServiceImpl apiVersionsService;

    public ApiVersionResponseAggregator() {
        this(new ApiVersionsServiceImpl());
    }

    public ApiVersionResponseAggregator(ApiVersionsServiceImpl apiVersionsService) {
        this.apiVersionsService = Objects.requireNonNull(apiVersionsService, "apiVersionsService cannot be null");
    }

    @Override
    public CompletionStage<AggregateResponse<ApiVersionsResponseData>> aggregate(
            ResponseAggregationContext<ApiVersionsResponseData> context) {

        Objects.requireNonNull(context, "aggregation context cannot be null");

        var responses = context.responses();
        if (responses.isEmpty()) {
            String message = "No API versions responses to aggregate";
            LOGGER.error(message);
            return CompletableFuture.failedFuture(new IllegalArgumentException(message));
        }

        // Debug logging
        if (LOGGER.isDebugEnabled()) {
            responses.forEach((targetCluster, response) -> LOGGER.debug("API versions response from cluster {}: code={}, apis={}",
                    targetCluster, response.errorCode(), response.apiKeys().size()));
        }

        // Find if any response has an error and identify the one with fewest APIs
        TargetCluster errorCluster = null;
        short errorCode = 0;
        int minApiCount = Integer.MAX_VALUE;

        for (Map.Entry<TargetCluster, ApiVersionsResponseData> entry : responses.entrySet()) {
            ApiVersionsResponseData response = entry.getValue();
            if (response.errorCode() != 0) {
                int apiCount = response.apiKeys().size();
                if (apiCount < minApiCount) {
                    minApiCount = apiCount;
                    errorCluster = entry.getKey();
                    errorCode = response.errorCode();
                }
                LOGGER.warn("API versions error from cluster {}: code={}, apis={}",
                        entry.getKey().name(), response.errorCode(), apiCount);
            }
        }

        // Negotiate API versions across all clusters
        Map<Short, ApiVersionsResponseData.ApiVersion> mergedVersions = getProxyApiVersions();

        for (ApiVersionsResponseData response : responses.values()) {
            intersectWithClusterResponse(mergedVersions, response);
        }

        // Build response with negotiated versions
        ApiVersionsResponseData mergedResponse = new ApiVersionsResponseData();
        ApiVersionsResponseData.ApiVersionCollection apiVersions = new ApiVersionsResponseData.ApiVersionCollection();
        apiVersions.addAll(mergedVersions.values());
        mergedResponse.setErrorCode(errorCode); // Use error code from cluster with the fewest APIs, or 0 if no errors
        mergedResponse.setApiKeys(apiVersions);

        LOGGER.debug("API versions aggregation complete: {} APIs negotiated across {} clusters{}",
                mergedVersions.size(), responses.size(),
                errorCode != 0 ? " (with error code " + errorCode + ")" : "");

        ResponseHeaderData responseHeader = errorCluster != null
                ? context.headers().getOrDefault(errorCluster, new ResponseHeaderData())
                : context.firstHeader();

        return CompletableFuture.completedFuture(
                new AggregateResponse<>(responseHeader, mergedResponse));
    }

    /**
     * Creates a map of API version information indexed by API key for efficient lookup.
     */
    private Map<Short, ApiVersionsResponseData.ApiVersion> createVersionMap(ApiVersionsResponseData response) {
        return response.apiKeys().stream()
                .collect(Collectors.toMap(ApiVersionsResponseData.ApiVersion::apiKey, v -> v));
    }

    /**
     * Returns the set of APIs supported by the kafka-client library used by this proxy.
     * This is the starting point before negotiation with actual clusters.
     *
     * @return Map of API key to version information
     */
    private Map<Short, ApiVersionsResponseData.ApiVersion> getProxyApiVersions() {
        Map<Short, ApiVersionsResponseData.ApiVersion> versionMap = new HashMap<>();

        for (ApiKeys apiKey : ApiKeys.values()) {
            versionMap.merge(
                    apiKey.id,
                    new ApiVersionsResponseData.ApiVersion()
                            .setApiKey(apiKey.id)
                            .setMinVersion(apiKey.messageType.lowestSupportedVersion())
                            .setMaxVersion(apiVersionsService.latestVersion(apiKey)),
                    (existing, incoming) -> existing // Keep first occurrence
            );
        }

        LOGGER.debug("Proxy supports {} APIs", versionMap.size());
        return versionMap;
    }

    /**
     * Intersects merged API versions with a single cluster response.
     * Updates mergedVersions in place to contain the intersection.
     */
    private void intersectWithClusterResponse(Map<Short, ApiVersionsResponseData.ApiVersion> mergedVersions,
                                              ApiVersionsResponseData clusterResponse) {
        Map<Short, ApiVersionsResponseData.ApiVersion> clusterVersionMap = createVersionMap(clusterResponse);

        for (Map.Entry<Short, ApiVersionsResponseData.ApiVersion> entry : mergedVersions.entrySet()) {
            Short apiKey = entry.getKey();
            ApiVersionsResponseData.ApiVersion mergedVersion = entry.getValue();
            ApiVersionsResponseData.ApiVersion clusterVersion = clusterVersionMap.get(apiKey);

            if (clusterVersion != null) {
                // Negotiate: take the intersection
                // Min version: most restrictive (maximum of minimums)
                // Max version: most restrictive (minimum of maximums)
                short newMinVersion = (short) Math.max(mergedVersion.minVersion(), clusterVersion.minVersion());
                short newMaxVersion = (short) Math.min(mergedVersion.maxVersion(), clusterVersion.maxVersion());

                LOGGER.trace("API {} negotiated: min=[{},{}]→{}, max=[{},{}]→{}",
                        apiKey,
                        mergedVersion.minVersion(), clusterVersion.minVersion(), newMinVersion,
                        mergedVersion.maxVersion(), clusterVersion.maxVersion(), newMaxVersion);

                mergedVersion.setMinVersion(newMinVersion);
                mergedVersion.setMaxVersion(newMaxVersion);
            }
            else {
                // API not supported by this cluster: mark as unsupported
                LOGGER.trace("API {} not supported by cluster, marking unsupported", apiKey);
                mergedVersion.setMinVersion((short) 0);
                mergedVersion.setMaxVersion((short) 0);
            }
        }
    }
}