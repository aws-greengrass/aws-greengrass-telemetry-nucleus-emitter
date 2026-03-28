/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * This class is a modified version of
 * https://github.com/aws-greengrass/aws-greengrass-nucleus/blob/main/src/main/java/com/aws/greengrass/telemetry/
 * SystemMetricsEmitter.java
 * Extended with disk and network metrics collection.
 */

package com.aws.greengrass.telemetry.nucleus.emitter.metrics;

import com.aws.greengrass.telemetry.impl.Metric;
import com.aws.greengrass.telemetry.models.TelemetryAggregation;
import com.aws.greengrass.telemetry.models.TelemetryUnit;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.NetworkIF;
import oshi.software.os.OSFileStore;

import java.net.SocketException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SystemMetricsEmitter extends PeriodicMetricsEmitter {
    private static final int MB_CONVERTER = 1024 * 1024;
    private static final int PERCENTAGE_CONVERTER = 100;
    public static final String NAMESPACE = "SystemMetrics";
    private static final SystemInfo systemInfo = new SystemInfo();
    private static final CentralProcessor cpu = systemInfo.getHardware().getProcessor();
    private long[] previousTicks = new long[CentralProcessor.TickType.values().length];
    // Single-threaded access from scheduled executor; no concurrent map needed
    private final Map<String, NetworkSnapshot> previousNetworkCounters = new HashMap<>();
    private final boolean detailedMetrics;
    private final List<String> excludeMounts;
    private final List<String> excludeInterfaces;

    /**
     * Creates a SystemMetricsEmitter with detailed metrics configuration.
     *
     * @param detailedMetrics   whether to collect disk and network metrics
     * @param excludeMounts     filesystem types to exclude from disk metrics (e.g. "tmpfs",
     *                          "devtmpfs"), matched against {@link OSFileStore#getType()},
     *                          not the mount path
     * @param excludeInterfaces interface names to exclude from network metrics
     */
    public SystemMetricsEmitter(boolean detailedMetrics, List<String> excludeMounts,
                                List<String> excludeInterfaces) {
        super();
        this.detailedMetrics = detailedMetrics;
        this.excludeMounts = Collections.unmodifiableList(new ArrayList<>(excludeMounts));
        this.excludeInterfaces = Collections.unmodifiableList(new ArrayList<>(excludeInterfaces));
    }

    /**
     * Retrieve system metrics. Includes disk and network metrics when detailed mode is enabled.
     *
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
                .aggregation(TelemetryAggregation.Count)
                .value(systemInfo.getOperatingSystem().getFileSystem().getOpenFileDescriptors())
                .timestamp(timestamp)
                .build();
        metricsList.add(metric);

        GlobalMemory memory = systemInfo.getHardware().getMemory();
        metric = Metric.builder()
                .namespace(NAMESPACE)
                .name("SystemMemUsage")
                .unit(TelemetryUnit.Megabytes)
                .aggregation(TelemetryAggregation.Count)
                .value((memory.getTotal() - memory.getAvailable()) / MB_CONVERTER)
                .timestamp(timestamp)
                .build();
        metricsList.add(metric);

        if (detailedMetrics) {
            metricsList.addAll(collectDiskMetrics(timestamp));
            metricsList.addAll(collectNetworkMetrics(timestamp));
        }

        return metricsList;
    }

    private List<Metric> collectDiskMetrics(long timestamp) {
        List<Metric> metrics = new ArrayList<>();
        for (OSFileStore fs : systemInfo.getOperatingSystem().getFileSystem().getFileStores()) {
            if (excludeMounts.contains(fs.getType())) {
                continue;
            }
            long total = fs.getTotalSpace();
            long available = fs.getUsableSpace();
            double usagePercent = total > 0 ? ((double) (total - available) / total) * PERCENTAGE_CONVERTER : 0;

            metrics.add(Metric.builder().namespace(NAMESPACE)
                    .name("DiskUsagePercent_" + fs.getMount())
                    .unit(TelemetryUnit.Percent).aggregation(TelemetryAggregation.Average)
                    .value(usagePercent).timestamp(timestamp).build());
            metrics.add(Metric.builder().namespace(NAMESPACE)
                    .name("DiskTotalBytes_" + fs.getMount())
                    .unit(TelemetryUnit.Bytes).aggregation(TelemetryAggregation.Count)
                    .value(total).timestamp(timestamp).build());
            metrics.add(Metric.builder().namespace(NAMESPACE)
                    .name("DiskAvailableBytes_" + fs.getMount())
                    .unit(TelemetryUnit.Bytes).aggregation(TelemetryAggregation.Count)
                    .value(available).timestamp(timestamp).build());
        }
        return metrics;
    }

    private List<Metric> collectNetworkMetrics(long timestamp) {
        List<Metric> metrics = new ArrayList<>();
        for (NetworkIF nif : systemInfo.getHardware().getNetworkIFs()) {
            String name = nif.getName();
            try {
                if (nif.queryNetworkInterface().isLoopback()) {
                    continue;
                }
            } catch (SocketException e) {
                // Interface unavailable (e.g. being removed); skip this cycle, next call retries
                continue;
            }
            if (excludeInterfaces.contains(name)) {
                continue;
            }
            if (!nif.updateAttributes()) {
                continue;
            }
            NetworkSnapshot current = new NetworkSnapshot(nif);
            NetworkSnapshot prev = previousNetworkCounters.put(name, current);
            if (prev == null) {
                continue;
            }
            long timeDelta = current.timestamp - prev.timestamp;
            double seconds = timeDelta / 1000.0;

            addNetworkMetric(metrics, "BytesRecvPerSec", name, TelemetryUnit.Bytes,
                    Math.max(0, current.bytesRecv - prev.bytesRecv) / seconds, timestamp);
            addNetworkMetric(metrics, "BytesSentPerSec", name, TelemetryUnit.Bytes,
                    Math.max(0, current.bytesSent - prev.bytesSent) / seconds, timestamp);
            addNetworkMetric(metrics, "PacketsRecvPerSec", name, TelemetryUnit.Count,
                    Math.max(0, current.packetsRecv - prev.packetsRecv) / seconds, timestamp);
            addNetworkMetric(metrics, "PacketsSentPerSec", name, TelemetryUnit.Count,
                    Math.max(0, current.packetsSent - prev.packetsSent) / seconds, timestamp);
            addNetworkMetric(metrics, "InErrorsPerSec", name, TelemetryUnit.Count,
                    Math.max(0, current.inErrors - prev.inErrors) / seconds, timestamp);
            addNetworkMetric(metrics, "OutErrorsPerSec", name, TelemetryUnit.Count,
                    Math.max(0, current.outErrors - prev.outErrors) / seconds, timestamp);
            addNetworkMetric(metrics, "InDropsPerSec", name, TelemetryUnit.Count,
                    Math.max(0, current.inDrops - prev.inDrops) / seconds, timestamp);
        }
        return metrics;
    }

    private void addNetworkMetric(List<Metric> metrics, String metricName, String ifName,
                                  TelemetryUnit unit, double value, long timestamp) {
        metrics.add(Metric.builder().namespace(NAMESPACE).name(metricName + "_" + ifName)
                .unit(unit).aggregation(TelemetryAggregation.Average)
                .value(value).timestamp(timestamp).build());
    }

    private static class NetworkSnapshot {
        final long bytesRecv;
        final long bytesSent;
        final long packetsRecv;
        final long packetsSent;
        final long inErrors;
        final long outErrors;
        final long inDrops;
        final long timestamp;

        NetworkSnapshot(NetworkIF nif) {
            this.bytesRecv = nif.getBytesRecv();
            this.bytesSent = nif.getBytesSent();
            this.packetsRecv = nif.getPacketsRecv();
            this.packetsSent = nif.getPacketsSent();
            this.inErrors = nif.getInErrors();
            this.outErrors = nif.getOutErrors();
            this.inDrops = nif.getInDrops();
            this.timestamp = nif.getTimeStamp();
        }
    }
}
