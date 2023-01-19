/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter.alarms;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Builder
@Value
public class Threshold {

    Condition condition;
    long period;
    TimeUnit periodTimeUnit;
    double value;
    int datapoints;
    int evaluationPeriods;


    public enum Condition {
        GREATER_OR_EQ(">="),
        GREATER(">"),
        LESS_OR_EQ("<="),
        LESS("<");

        @Getter(AccessLevel.PRIVATE)
        private final String expr;

        Condition(String expr) {
            this.expr = expr;
        }

        public static Optional<Condition> fromExpr(String expr) {
            return Arrays.stream(Condition.values())
                    .filter(c -> c.getExpr().equals(expr))
                    .findFirst();
        }
    }
}
