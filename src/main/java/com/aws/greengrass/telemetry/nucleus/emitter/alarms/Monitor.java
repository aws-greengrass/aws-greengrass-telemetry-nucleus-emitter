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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class Monitor {

    private final Deque<Metric> datapoints = new ArrayDeque<>();
    private final Supplier<Metric> datapoint;
    private final Threshold threshold;
    private final BiConsumer<State, List<Metric>> callback;
    private final ScheduledExecutorService ses;
    private final Object taskLock = new Object();

    private ScheduledFuture<?> task;

    @Builder
    public Monitor(Supplier<Metric> datapoint,
                   Threshold threshold,
                   BiConsumer<State, List<Metric>> callback,
                   ScheduledExecutorService ses) {
        this.datapoint = datapoint;
        this.threshold = threshold;
        this.callback = callback;
        this.ses = ses;
    }

    public void start() {
        synchronized (taskLock) {
            if (task != null) {
                task.cancel(false);
            }
            task = ses.scheduleAtFixedRate(
                    () -> callback.accept(evaluate(), new ArrayList<>(datapoints)),
                    0,
                    threshold.getPeriod(),
                    threshold.getPeriodTimeUnit()
            );
        }
    }

    public void stop() {
        synchronized (taskLock) {
            if (task != null) {
                task.cancel(false);
            }
        }
    }

    public State evaluate() {
        datapoints.addLast(datapoint.get());

        while (datapoints.size() > threshold.getEvaluationPeriods()) {
            datapoints.removeFirst();
        }

        long numBreaching = datapoints.stream().filter(this::isBreaching).count();
        if (numBreaching >= threshold.getDatapoints()) {
            return State.ALARM;
        }

        return State.OK;
    }

    private boolean isBreaching(Metric metric) {
        if (!(metric.getValue() instanceof Number)) {
            // unable to evaluate because metric is not a number,
            // treat as breaching.
            return true;
        }
        double datapoint = ((Number) metric.getValue()).doubleValue();
        switch (threshold.getCondition()) {
            case LESS:
                return datapoint < threshold.getValue();
            case GREATER:
                return datapoint > threshold.getValue();
            case LESS_OR_EQ:
                return datapoint <= threshold.getValue();
            case GREATER_OR_EQ:
                return datapoint >= threshold.getValue();
            default:
                // unrecognized condition,
                // treat as breaching
                return true;
        }
    }

    public enum State {
        OK,
        ALARM
    }
}
