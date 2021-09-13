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
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE, onConstructor = @__({ @Inject}))
public class SystemMetricsEmitter extends PeriodicMetricsEmitter {
    private static final int MB_CONVERTER = 1024 * 1024;
    private static final int PERCENTAGE_CONVERTER = 100;
    public static final String NAMESPACE = "SystemMetrics";
    private static final SystemInfo systemInfo = new SystemInfo();
    private static final CentralProcessor cpu = systemInfo.getHardware().getProcessor();
    private long[] previousTicks = new long[CentralProcessor.TickType.values().length];

    /**
     * Retrieve kernel component state metrics.
     * @return a list of {@link Metric}
     */
    @Override
    public List<Metric> getMetrics() {
        List<Metric> metricsList = new ArrayList<>();
        long timestamp = Instant.now().toEpochMilli();

        Metric metric = Metric.builder()
                .namespace(NAMESPACE)
                .name("CpuUsage")
                .unit(TelemetryUnit.Percent)
                .aggregation(TelemetryAggregation.Average)
                .value(cpu.getSystemCpuLoadBetweenTicks(previousTicks) * PERCENTAGE_CONVERTER)
                .timestamp(timestamp)
                .build();
        previousTicks = cpu.getSystemCpuLoadTicks();
        metricsList.add(metric);

        metric = Metric.builder()
                .namespace(NAMESPACE)
                .name("TotalNumberOfFDs")
                .unit(TelemetryUnit.Count)
                .aggregation(TelemetryAggregation.Average)
                .value(systemInfo.getOperatingSystem().getFileSystem().getOpenFileDescriptors())
                .timestamp(timestamp)
                .build();
        metricsList.add(metric);

        GlobalMemory memory = systemInfo.getHardware().getMemory();
        metric = Metric.builder()
                .namespace(NAMESPACE)
                .name("SystemMemUsage")
                .unit(TelemetryUnit.Megabytes)
                .aggregation(TelemetryAggregation.Average)
                .value((memory.getTotal() - memory.getAvailable()) / MB_CONVERTER)
                .timestamp(timestamp)
                .build();
        metricsList.add(metric);

        return metricsList;
    }
}

