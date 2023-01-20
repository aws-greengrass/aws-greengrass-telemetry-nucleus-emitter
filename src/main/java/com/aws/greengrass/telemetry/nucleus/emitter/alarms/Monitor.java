/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter.alarms;

import com.aws.greengrass.telemetry.impl.Metric;
import lombok.Builder;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.Supplier;

public class Monitor {

    private final Deque<Metric> datapoints = new ArrayDeque<>();
    private final Supplier<Metric> datapoint;
    private final Supplier<Threshold> threshold;

    @Builder
    public Monitor(Supplier<Metric> datapoint,
                   Supplier<Threshold> threshold) {
        this.datapoint = datapoint;
        this.threshold = threshold;
    }

    public List<Metric> getDatapoints() {
        return new ArrayList<>(datapoints);
    }

    public State evaluate() {
        datapoints.addLast(datapoint.get());

        Threshold currThreshold = getThreshold();
        if (currThreshold == null) {
            return State.UNKNOWN;
        }

        while (datapoints.size() > currThreshold.getEvaluationPeriods()) {
            datapoints.removeFirst();
        }

        long numBreaching = datapoints.stream().filter(datapoint -> isBreaching(currThreshold, datapoint)).count();
        if (numBreaching >= currThreshold.getDatapoints()) {
            return State.ALARM;
        }

        return State.OK;
    }

    private boolean isBreaching(Threshold currThreshold, Metric metric) {
        if (!(metric.getValue() instanceof Number)) {
            // unable to evaluate because metric is not a number,
            // treat as breaching.
            return true;
        }
        double datapoint = ((Number) metric.getValue()).doubleValue();
        switch (currThreshold.getCondition()) {
            case LESS:
                return datapoint < currThreshold.getValue();
            case GREATER:
                return datapoint > currThreshold.getValue();
            case LESS_OR_EQ:
                return datapoint <= currThreshold.getValue();
            case GREATER_OR_EQ:
                return datapoint >= currThreshold.getValue();
            default:
                // unrecognized condition,
                // treat as breaching
                return true;
        }
    }

    public Threshold getThreshold() {
        return threshold.get();
    }

    public enum State {
        OK,
        ALARM,
        UNKNOWN
    }
}
