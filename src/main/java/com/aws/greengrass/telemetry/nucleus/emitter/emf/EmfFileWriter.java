/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter.emf;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.telemetry.impl.Metric;
import com.aws.greengrass.telemetry.models.TelemetryUnit;
import com.fasterxml.jackson.core.JsonProcessingException;
import software.amazon.cloudwatchlogs.emf.model.DimensionSet;
import software.amazon.cloudwatchlogs.emf.model.MetricsContext;
import software.amazon.cloudwatchlogs.emf.model.Unit;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Serializes metrics in EMF (Embedded Metric Format) and writes them via a
 * raw Greengrass logger (no envelope). File rotation is handled by the GG
 * logging framework. The separate LogManager component handles upload to
 * CloudWatch Logs, where EMF is auto-extracted into CloudWatch Metrics.
 */
public class EmfFileWriter {

    private static final Logger logger = LogManager.getLogger(EmfFileWriter.class);

    private static final Map<TelemetryUnit, Unit> UNIT_MAP;

    static {
        Map<TelemetryUnit, Unit> m = new LinkedHashMap<>();
        m.put(TelemetryUnit.Percent, Unit.PERCENT);
        m.put(TelemetryUnit.Bytes, Unit.BYTES);
        m.put(TelemetryUnit.Megabytes, Unit.MEGABYTES);
        m.put(TelemetryUnit.Count, Unit.COUNT);
        UNIT_MAP = Collections.unmodifiableMap(m);
    }

    private final String thingName;
    private final Logger emfLogger;

    /**
     * Creates an EmfFileWriter.
     *
     * @param thingName      thing name for dimensions
     * @param outputDirectory directory for EMF output files
     */
    public EmfFileWriter(String thingName, Path outputDirectory) {
        this.thingName = thingName;
        this.emfLogger = LogManager.getRawLogger("emf-metrics", outputDirectory);
    }

    /**
     * Serializes metrics to EMF JSON and writes them via the raw logger.
     *
     * @param metrics list of metrics to write
     */
    public void write(List<Metric> metrics) {
        if (metrics == null || metrics.isEmpty()) {
            return;
        }
        Map<String, List<Metric>> byNamespace = groupByNamespace(metrics);
        for (Map.Entry<String, List<Metric>> entry : byNamespace.entrySet()) {
            MetricsContext context = new MetricsContext();
            context.setNamespace(entry.getKey());
            context.putDimension(DimensionSet.of("ThingName", thingName));
            // Use the collection timestamp from the first metric in this namespace
            List<Metric> nsMetrics = entry.getValue();
            if (!nsMetrics.isEmpty() && nsMetrics.get(0).getTimestamp() > 0) {
                context.setTimestamp(Instant.ofEpochMilli(nsMetrics.get(0).getTimestamp()));
            }
            for (Metric metric : entry.getValue()) {
                if (!(metric.getValue() instanceof Number)) {
                    continue;
                }
                context.putMetric(metric.getName(),
                        ((Number) metric.getValue()).doubleValue(),
                        toEmfUnit(metric.getUnit()));
            }
            try {
                for (String line : context.serialize()) {
                    emfLogger.atInfo().log(line);
                }
            } catch (JsonProcessingException e) {
                logger.error("Failed to serialize EMF metrics", e);
            }
        }
    }

    private Map<String, List<Metric>> groupByNamespace(List<Metric> metrics) {
        Map<String, List<Metric>> result = new LinkedHashMap<>();
        for (Metric m : metrics) {
            result.computeIfAbsent(m.getNamespace(), k -> new ArrayList<>()).add(m);
        }
        return result;
    }

    private static Unit toEmfUnit(TelemetryUnit unit) {
        return UNIT_MAP.getOrDefault(unit, Unit.NONE);
    }
}
