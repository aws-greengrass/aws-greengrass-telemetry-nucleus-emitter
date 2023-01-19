/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter.metrics;

import com.aws.greengrass.telemetry.impl.Metric;
import com.aws.greengrass.telemetry.models.TelemetryAggregation;
import com.aws.greengrass.telemetry.models.TelemetryUnit;
import lombok.RequiredArgsConstructor;
import oshi.hardware.CentralProcessor;

import java.time.Instant;
import java.util.function.Supplier;

import static com.aws.greengrass.telemetry.nucleus.emitter.utils.System.CPU;

@RequiredArgsConstructor
public class CpuMetric implements Supplier<Metric> {
    private static final String NAME = "CpuUsage";
    private long[] previousTicks = new long[CentralProcessor.TickType.values().length];
    private final String namespace;

    @Override
    public Metric get() {
        long[] prevTicks = previousTicks;
        previousTicks = CPU.getSystemCpuLoadTicks();

        return Metric.builder()
                .namespace(namespace)
                .name(NAME)
                .unit(TelemetryUnit.Percent)
                .aggregation(TelemetryAggregation.Average)
                .value(CPU.getSystemCpuLoadBetweenTicks(prevTicks) * 100)
                .timestamp(Instant.now().toEpochMilli())
                .build();
    }
}
