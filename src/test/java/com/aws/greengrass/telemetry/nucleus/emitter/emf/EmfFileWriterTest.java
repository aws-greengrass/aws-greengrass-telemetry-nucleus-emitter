/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter.emf;

import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logging.impl.config.LogConfig;
import com.aws.greengrass.logging.impl.config.LogFormat;
import com.aws.greengrass.telemetry.impl.Metric;
import com.aws.greengrass.telemetry.models.TelemetryAggregation;
import com.aws.greengrass.telemetry.models.TelemetryUnit;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith({GGExtension.class})
class EmfFileWriterTest {

    private static final String THING_NAME = "TestThing";
    private static final String NAMESPACE = "SystemMetrics";

    @TempDir
    static Path tempDir;

    @AfterAll
    static void cleanup() {
        // Close the emf-metrics logger context so Windows can delete tempDir
        LogConfig config = LogManager.getLogConfigurations().get("emf-metrics");
        if (config != null) {
            config.closeContext();
        }
    }

    private Metric buildMetric(String name, TelemetryUnit unit, Object value) {
        return Metric.builder()
                .namespace(NAMESPACE)
                .name(name)
                .unit(unit)
                .aggregation(TelemetryAggregation.Average)
                .value(value)
                .timestamp(System.currentTimeMillis())
                .build();
    }

    @Test
    void GIVEN_system_metrics_WHEN_write_THEN_emf_json_written_raw() {
        EmfFileWriter writer = new EmfFileWriter(THING_NAME, tempDir);
        List<Metric> metrics = Arrays.asList(
                buildMetric("CpuUsage", TelemetryUnit.Percent, 45.2),
                buildMetric("SystemMemUsage", TelemetryUnit.Megabytes, 512.0));
        writer.write(metrics);

        // The raw logger file path is managed by the GG logging framework.
        // Verify via LogManager that the config was created with RAW format.
        LogConfig emfConfig = LogManager.getLogConfigurations().get("emf-metrics");
        assertNotNull(emfConfig, "emf-metrics logger config should exist");
        assertEquals(LogFormat.RAW, emfConfig.getFormat(),
                "EMF logger should use RAW format");
    }

    @Test
    void GIVEN_empty_metrics_WHEN_write_THEN_no_error() {
        EmfFileWriter writer = new EmfFileWriter(THING_NAME, tempDir);
        assertDoesNotThrow(() -> writer.write(Collections.emptyList()));
    }

    @Test
    void GIVEN_null_metrics_WHEN_write_THEN_no_error() {
        EmfFileWriter writer = new EmfFileWriter(THING_NAME, tempDir);
        assertDoesNotThrow(() -> writer.write(null));
    }

    @Test
    void GIVEN_multiple_namespaces_WHEN_write_THEN_no_error() {
        EmfFileWriter writer = new EmfFileWriter(THING_NAME, tempDir);
        List<Metric> metrics = Arrays.asList(
                buildMetric("CpuUsage", TelemetryUnit.Percent, 10.0),
                Metric.builder().namespace("GreengrassComponents")
                        .name("NumberOfComponentsRunning").unit(TelemetryUnit.Count)
                        .aggregation(TelemetryAggregation.Average)
                        .value(5.0).timestamp(System.currentTimeMillis()).build());
        assertDoesNotThrow(() -> writer.write(metrics));
    }

    @Test
    void GIVEN_all_unit_types_WHEN_write_THEN_no_error() {
        EmfFileWriter writer = new EmfFileWriter(THING_NAME, tempDir);
        List<Metric> metrics = Arrays.asList(
                buildMetric("pct", TelemetryUnit.Percent, 1.0),
                buildMetric("bytes", TelemetryUnit.Bytes, 2.0),
                buildMetric("mb", TelemetryUnit.Megabytes, 3.0),
                buildMetric("cnt", TelemetryUnit.Count, 4.0));
        assertDoesNotThrow(() -> writer.write(metrics));
    }

    @Test
    void GIVEN_non_number_value_WHEN_write_THEN_skipped_no_error() {
        EmfFileWriter writer = new EmfFileWriter(THING_NAME, tempDir);
        List<Metric> metrics = Arrays.asList(
                buildMetric("CpuUsage", TelemetryUnit.Percent, "not_a_number"),
                buildMetric("MemUsage", TelemetryUnit.Megabytes, 512.0));
        assertDoesNotThrow(() -> writer.write(metrics));
    }

    @Test
    void GIVEN_multiple_writes_WHEN_write_THEN_no_error() {
        EmfFileWriter writer = new EmfFileWriter(THING_NAME, tempDir);
        List<Metric> metrics = Collections.singletonList(
                buildMetric("CpuUsage", TelemetryUnit.Percent, 10.0));
        assertDoesNotThrow(() -> {
            writer.write(metrics);
            writer.write(metrics);
            writer.write(metrics);
        });
    }
}
