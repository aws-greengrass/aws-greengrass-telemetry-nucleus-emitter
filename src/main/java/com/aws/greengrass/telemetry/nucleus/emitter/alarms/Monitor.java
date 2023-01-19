/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter.alarms;

import lombok.AllArgsConstructor;
import lombok.Setter;

import java.util.ArrayDeque;
import java.util.Deque;

@AllArgsConstructor
public class Monitor {

    // TODO is this the right structure
    private final Deque<Double> datapoints = new ArrayDeque<>();

    @Setter
    private Threshold threshold;

    public State evaluate(double newDatapoint) {
        // TODO window of datapoints
        datapoints.add(newDatapoint);
        // TODO factor in evaluation periods, etc
        boolean allBreaching = datapoints.stream().allMatch(this::isBreaching);
        return allBreaching ? State.ALARM : State.OK;
    }

    private boolean isBreaching(double datapoint) {
        switch (threshold.getCondition()) {
            case LESS: return datapoint < threshold.getValue();
            case GREATER: return datapoint > threshold.getValue();
            case LESS_OR_EQ: return datapoint <= threshold.getValue();
            case GREATER_OR_EQ: return datapoint >= threshold.getValue();
            default: return false;
        }
    }

    public enum State {
        OK,
        ALARM
    }
}
