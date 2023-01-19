/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * This class is a modified version of
 * https://github.com/aws-greengrass/aws-greengrass-nucleus/blob/main/src/main/java/com/aws/greengrass/telemetry/
 * SystemMetricsEmitter.java
 * and needs to be removed upon the release of Greengrass 2.5.0.
 */

package com.aws.greengrass.telemetry.nucleus.emitter.metrics;

import com.aws.greengrass.telemetry.impl.Metric;
import com.aws.greengrass.telemetry.models.TelemetryAggregation;
import com.aws.greengrass.telemetry.models.TelemetryUnit;
import oshi.hardware.GlobalMemory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static com.aws.greengrass.telemetry.nucleus.emitter.utils.System.SYSTEM_INFO;

public class SystemMetricsEmitter extends PeriodicMetricsEmitter {
    private static final int MB_CONVERTER = 1024 * 1024;
    public static final String NAMESPACE = "SystemMetrics";

    private final CpuMetric cpuMetric;
    private final MemoryMetric memoryMetric;
    private final DiskMetric diskMetric;

    public SystemMetricsEmitter() {
        this.cpuMetric = new CpuMetric(NAMESPACE);
        this.memoryMetric = new MemoryMetric(NAMESPACE);
        this.diskMetric = new DiskMetric(NAMESPACE);
    }

    /**
     * Retrieve kernel component state metrics.
     *
     * @return a list of {@link Metric}
     */
    @Override
    public List<Metric> getMetrics() {
        List<Metric> metricsList = new ArrayList<>();
        long timestamp = Instant.now().toEpochMilli();

        Metric metric = cpuMetric.get();
        metric.setTimestamp(timestamp);
        metricsList.add(metric);

        metric = Metric.builder()
                .namespace(NAMESPACE)
                .name("TotalNumberOfFDs")
                .unit(TelemetryUnit.Count)
                .aggregation(TelemetryAggregation.Count)
                .value(SYSTEM_INFO.getOperatingSystem().getFileSystem().getOpenFileDescriptors())
                .timestamp(timestamp)
                .build();
        metricsList.add(metric);

        GlobalMemory memory = SYSTEM_INFO.getHardware().getMemory();
        metric = Metric.builder()
                .namespace(NAMESPACE)
                .name("SystemMemUsage")
                .unit(TelemetryUnit.Megabytes)
                .aggregation(TelemetryAggregation.Count)
                .value((memory.getTotal() - memory.getAvailable()) / MB_CONVERTER)
                .timestamp(timestamp)
                .build();
        metricsList.add(metric);

        metric = memoryMetric.get(memory);
        metricsList.add(metric);

        metric = diskMetric.get();
        metricsList.add(metric);

        return metricsList;
    }
}

