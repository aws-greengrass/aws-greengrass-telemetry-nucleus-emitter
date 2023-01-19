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
import static com.aws.greengrass.telemetry.nucleus.emitter.utils.System.SYSTEM_INFO;

@RequiredArgsConstructor
public class DiskMetric implements Supplier<Metric> {
    public static final String NAME = "SystemDiskUsagePercentage";
    private final String namespace;

    @Override
    public Metric get() {
        double maxUsedSpacePercentage =
                SYSTEM_INFO.getOperatingSystem().getFileSystem().getFileStores(true).stream()
                        .map(d -> 1.0 - ((double) d.getUsableSpace() / d.getTotalSpace()))
                        .mapToDouble(Double::doubleValue)
                        .max()
                        .getAsDouble();

        return Metric.builder()
                .namespace(namespace)
                .name(NAME)
                .unit(TelemetryUnit.Percent)
                .aggregation(TelemetryAggregation.Maximum)
                .value(maxUsedSpacePercentage * 100)
                .timestamp(Instant.now().toEpochMilli())
                .build();
    }
}
