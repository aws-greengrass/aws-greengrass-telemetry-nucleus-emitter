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
import oshi.hardware.GlobalMemory;

import java.time.Instant;
import java.util.function.Supplier;

import static com.aws.greengrass.telemetry.nucleus.emitter.utils.System.CPU;
import static com.aws.greengrass.telemetry.nucleus.emitter.utils.System.SYSTEM_INFO;

@RequiredArgsConstructor
public class MemoryMetric implements Supplier<Metric> {
    public static final String NAME = "SystemMemUsagePercentage";
    private final String namespace;

    @Override
    public Metric get() {
        return get(SYSTEM_INFO.getHardware().getMemory());
    }

    public Metric get(GlobalMemory memory) {
        return Metric.builder()
                .namespace(namespace)
                .name(NAME)
                .unit(TelemetryUnit.Percent)
                .aggregation(TelemetryAggregation.Maximum)
                .value(((double)(memory.getTotal() - memory.getAvailable()) / memory.getTotal()) * 100)
                .timestamp(Instant.now().toEpochMilli())
                .build();
    }
}
