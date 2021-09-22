/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * This class is a modified version of
 * https://github.com/aws-greengrass/aws-greengrass-nucleus/blob/main/src/main/java/com/aws/greengrass/lifecyclemanager/
 * KernelMetricsEmitter.java
 * and needs to be removed upon the release of Greengrass 2.5.0.
 */

package com.aws.greengrass.telemetry.nucleus.emitter.metrics;

import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.telemetry.impl.Metric;
import com.aws.greengrass.telemetry.models.TelemetryAggregation;
import com.aws.greengrass.telemetry.models.TelemetryUnit;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE, onConstructor = @__({ @Inject}))
public class KernelMetricsEmitter extends PeriodicMetricsEmitter {
    private static final String NAMESPACE = "GreengrassComponents";
    private final Kernel kernel;

    /**
     * Retrieve kernel component state metrics.
     * @return a list of {@link Metric}
     */
    @Override
    public List<Metric> getMetrics() {
        Map<State, Integer> stateCount = new EnumMap<>(State.class);
        Collection<GreengrassService> services = kernel.orderedDependencies();
        for (GreengrassService service : services) {
            stateCount.put(service.getState(), stateCount.getOrDefault(service.getState(), 0) + 1);
        }

        List<Metric> metricsList = new ArrayList<>();
        long timestamp = Instant.now().toEpochMilli();
        Metric metric = Metric.builder()
                .namespace(NAMESPACE)
                .name("NumberOfComponentsStarting")
                .unit(TelemetryUnit.Count)
                .aggregation(TelemetryAggregation.Count)
                .value(stateCount.getOrDefault(State.STARTING, 0))
                .timestamp(timestamp)
                .build();
        metricsList.add(metric);

        metric = Metric.builder()
                .namespace(NAMESPACE)
                .name("NumberOfComponentsInstalled")
                .unit(TelemetryUnit.Count)
                .aggregation(TelemetryAggregation.Count)
                .value(stateCount.getOrDefault(State.INSTALLED, 0))
                .timestamp(timestamp)
                .build();
        metricsList.add(metric);

        metric = Metric.builder()
                .namespace(NAMESPACE)
                .name("NumberOfComponentsStateless")
                .unit(TelemetryUnit.Count)
                .aggregation(TelemetryAggregation.Count)
                .value(stateCount.getOrDefault(State.STATELESS, 0))
                .timestamp(timestamp)
                .build();
        metricsList.add(metric);

        metric = Metric.builder()
                .namespace(NAMESPACE)
                .name("NumberOfComponentsStopping")
                .unit(TelemetryUnit.Count)
                .aggregation(TelemetryAggregation.Count)
                .value(stateCount.getOrDefault(State.STOPPING, 0))
                .timestamp(timestamp)
                .build();
        metricsList.add(metric);

        metric = Metric.builder()
                .namespace(NAMESPACE)
                .name("NumberOfComponentsBroken")
                .unit(TelemetryUnit.Count)
                .aggregation(TelemetryAggregation.Count)
                .value(stateCount.getOrDefault(State.BROKEN, 0))
                .timestamp(timestamp)
                .build();
        metricsList.add(metric);

        metric = Metric.builder()
                .namespace(NAMESPACE)
                .name("NumberOfComponentsRunning")
                .unit(TelemetryUnit.Count)
                .aggregation(TelemetryAggregation.Count)
                .value(stateCount.getOrDefault(State.RUNNING, 0))
                .timestamp(timestamp)
                .build();
        metricsList.add(metric);

        metric = Metric.builder()
                .namespace(NAMESPACE)
                .name("NumberOfComponentsErrored")
                .unit(TelemetryUnit.Count)
                .aggregation(TelemetryAggregation.Count)
                .value(stateCount.getOrDefault(State.ERRORED, 0))
                .timestamp(timestamp)
                .build();
        metricsList.add(metric);

        metric = Metric.builder()
                .namespace(NAMESPACE)
                .name("NumberOfComponentsNew")
                .unit(TelemetryUnit.Count)
                .aggregation(TelemetryAggregation.Count)
                .value(stateCount.getOrDefault(State.NEW, 0))
                .timestamp(timestamp)
                .build();
        metricsList.add(metric);

        metric = Metric.builder()
                .namespace(NAMESPACE)
                .name("NumberOfComponentsFinished")
                .unit(TelemetryUnit.Count)
                .aggregation(TelemetryAggregation.Count)
                .value(stateCount.getOrDefault(State.FINISHED, 0))
                .timestamp(timestamp)
                .build();
        metricsList.add(metric);

        return metricsList;
    }
}
