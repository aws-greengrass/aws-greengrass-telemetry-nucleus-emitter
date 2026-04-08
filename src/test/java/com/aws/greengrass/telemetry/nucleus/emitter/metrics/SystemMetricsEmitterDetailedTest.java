/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter.metrics;

import com.aws.greengrass.telemetry.impl.Metric;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import oshi.SystemInfo;
import oshi.hardware.NetworkIF;
import oshi.software.os.OSFileStore;

import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith({GGExtension.class})
class SystemMetricsEmitterDetailedTest {

    @Test
    void GIVEN_basic_emitter_WHEN_getMetrics_THEN_returns_only_3_metrics() {
        SystemMetricsEmitter emitter = new SystemMetricsEmitter(false,
                Collections.emptyList(), Collections.emptyList());
        List<Metric> metrics = emitter.getMetrics();
        assertEquals(3, metrics.size());
        assertFalse(metrics.stream().anyMatch(m -> m.getName().startsWith("Disk")));
        assertFalse(metrics.stream().anyMatch(m -> m.getName().contains("PerSec")));
    }

    @Test
    void GIVEN_detailed_emitter_WHEN_getMetrics_THEN_includes_disk_metrics() {
        SystemMetricsEmitter emitter = new SystemMetricsEmitter(
                true, Collections.emptyList(), Collections.emptyList());
        List<Metric> metrics = emitter.getMetrics();
        assertTrue(metrics.size() >= 3);
    }

    @Test
    void GIVEN_detailed_emitter_WHEN_getMetrics_THEN_disk_metrics_include_mount_in_name() {
        SystemMetricsEmitter emitter = new SystemMetricsEmitter(
                true, Collections.emptyList(), Collections.emptyList());
        List<Metric> metrics = emitter.getMetrics();
        long diskCount = metrics.stream()
                .filter(m -> m.getName().startsWith("Disk")).count();
        if (diskCount > 0) {
            assertTrue(metrics.stream()
                    .filter(m -> m.getName().startsWith("Disk"))
                    .allMatch(m -> m.getName().contains("_")));
        }
    }

    @Test
    void GIVEN_detailed_emitter_WHEN_first_call_THEN_no_network_rate_metrics() {
        SystemMetricsEmitter emitter = new SystemMetricsEmitter(
                true, Collections.emptyList(), Collections.emptyList());
        List<Metric> metrics = emitter.getMetrics();
        // First call: prev == null for every interface, so no rate metrics
        assertFalse(metrics.stream().anyMatch(m -> m.getName().startsWith("BytesRecvPerSec")));
    }

    @Test
    void GIVEN_detailed_emitter_WHEN_second_call_THEN_has_network_metrics() {
        SystemMetricsEmitter emitter = new SystemMetricsEmitter(
                true, Collections.emptyList(), Collections.emptyList());
        emitter.getMetrics();
        List<Metric> metrics = emitter.getMetrics();
        boolean hasNetwork = metrics.stream()
                .anyMatch(m -> m.getName().startsWith("BytesRecvPerSec"));
        if (hasNetwork) {
            assertTrue(metrics.stream().anyMatch(m -> m.getName().startsWith("BytesSentPerSec")));
            assertTrue(metrics.stream().anyMatch(m -> m.getName().startsWith("PacketsRecvPerSec")));
        }
    }

    @Test
    void GIVEN_excluded_mount_WHEN_getMetrics_THEN_mount_not_in_results() {
        SystemMetricsEmitter baseline = new SystemMetricsEmitter(
                true, Collections.emptyList(), Collections.emptyList());
        List<Metric> baseMetrics = baseline.getMetrics();
        long diskCount = baseMetrics.stream()
                .filter(m -> m.getName().startsWith("Disk")).count();
        if (diskCount > 0) {
            SystemInfo si = new SystemInfo();
            String firstType = si.getOperatingSystem().getFileSystem()
                    .getFileStores().get(0).getType();
            SystemMetricsEmitter filtered = new SystemMetricsEmitter(
                    true, Collections.singletonList(firstType),
                    Collections.emptyList());
            List<Metric> filteredMetrics = filtered.getMetrics();
            long filteredDiskCount = filteredMetrics.stream()
                    .filter(m -> m.getName().startsWith("Disk")).count();
            assertTrue(filteredDiskCount < diskCount,
                    "Excluding filesystem type '" + firstType
                            + "' should reduce disk metrics");
        }
    }

    @Test
    void GIVEN_loopback_excluded_WHEN_getMetrics_THEN_no_loopback_metrics() {
        // Loopback interfaces are always excluded via isLoopback()
        SystemMetricsEmitter emitter = new SystemMetricsEmitter(
                true, Collections.emptyList(), Collections.emptyList());
        emitter.getMetrics();
        List<Metric> metrics = emitter.getMetrics();
        SystemInfo si = new SystemInfo();
        for (NetworkIF nif : si.getHardware().getNetworkIFs()) {
            try {
                if (nif.queryNetworkInterface().isLoopback()) {
                    String suffix = "_" + nif.getName();
                    assertFalse(metrics.stream().anyMatch(
                            m -> m.getName().endsWith(suffix)),
                            "Loopback " + nif.getName() + " should be excluded");
                }
            } catch (SocketException e) {
                // skip
            }
        }
    }

    @Test
    void GIVEN_excluded_interface_WHEN_getMetrics_THEN_interface_not_in_results() {
        // Discover a real interface name to exclude
        SystemInfo si = new SystemInfo();
        List<NetworkIF> nifs = si.getHardware().getNetworkIFs();
        String ifToExclude = null;
        for (NetworkIF nif : nifs) {
            try {
                if (!nif.queryNetworkInterface().isLoopback()) {
                    ifToExclude = nif.getName();
                    break;
                }
            } catch (SocketException e) {
                // skip
            }
        }
        if (ifToExclude != null) {
            SystemMetricsEmitter filtered = new SystemMetricsEmitter(
                    true, Collections.emptyList(),
                    Collections.singletonList(ifToExclude));
            filtered.getMetrics();
            List<Metric> metrics = filtered.getMetrics();
            String suffix = "_" + ifToExclude;
            assertFalse(metrics.stream().anyMatch(m -> m.getName().endsWith(suffix)),
                    "Excluded interface should not appear in metrics");
        }
    }

    @Test
    void GIVEN_all_interfaces_excluded_WHEN_getMetrics_THEN_no_network_metrics() {
        // Exclude every non-loopback interface
        SystemInfo si = new SystemInfo();
        List<NetworkIF> nifs = si.getHardware().getNetworkIFs();
        List<String> allNames = new ArrayList<>();
        for (NetworkIF nif : nifs) {
            allNames.add(nif.getName());
        }
        SystemMetricsEmitter emitter = new SystemMetricsEmitter(
                true, Collections.emptyList(), allNames);
        emitter.getMetrics();
        List<Metric> metrics = emitter.getMetrics();
        assertFalse(metrics.stream().anyMatch(m -> m.getName().contains("PerSec")));
    }

    @Test
    void GIVEN_all_mounts_excluded_WHEN_getMetrics_THEN_no_disk_metrics() {
        // Exclude every filesystem type
        SystemInfo si = new SystemInfo();
        List<String> allTypes = new ArrayList<>();
        for (OSFileStore fs
                : si.getOperatingSystem().getFileSystem().getFileStores()) {
            if (!allTypes.contains(fs.getType())) {
                allTypes.add(fs.getType());
            }
        }
        SystemMetricsEmitter emitter = new SystemMetricsEmitter(
                true, allTypes, Collections.emptyList());
        List<Metric> metrics = emitter.getMetrics();
        assertFalse(metrics.stream().anyMatch(m -> m.getName().startsWith("Disk")));
    }

    @Test
    void GIVEN_basic_emitter_WHEN_called_twice_THEN_still_only_basic_metrics() {
        // Exercises detailedMetrics=false branch on repeated calls
        SystemMetricsEmitter emitter = new SystemMetricsEmitter(false,
                Collections.emptyList(), Collections.emptyList());
        emitter.getMetrics();
        List<Metric> metrics = emitter.getMetrics();
        assertEquals(3, metrics.size());
        assertFalse(metrics.stream().anyMatch(m -> m.getName().startsWith("Disk")));
        assertFalse(metrics.stream().anyMatch(m -> m.getName().contains("PerSec")));
    }
}
