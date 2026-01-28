/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.router.aggregator;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.protocol.ApiMessage;

import io.kroxylicious.proxy.internal.router.ResponseAggregationContext;

public class DefaultApiMessageAggregator implements ApiMessageAggregator<ApiMessage> {
    @Override
    public CompletionStage<AggregateResponse<ApiMessage>> aggregate(ResponseAggregationContext<ApiMessage> aggregationContext) {
        return CompletableFuture.completedFuture(new AggregateResponse<>(aggregationContext.firstHeader(), aggregationContext.firstBody()));
    }
}
