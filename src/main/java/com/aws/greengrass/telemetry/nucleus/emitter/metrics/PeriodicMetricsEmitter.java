/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * This class is a modified version of
 * https://github.com/aws-greengrass/aws-greengrass-nucleus/blob/main/src/main/java/com/aws/greengrass/telemetry
 * /PeriodicMetricsEmitter.java
 * and needs to be removed upon the release of Greengrass 2.5.0.
 */

package com.aws.greengrass.telemetry.nucleus.emitter.metrics;

import com.aws.greengrass.telemetry.impl.Metric;

import java.util.List;

public abstract class PeriodicMetricsEmitter {

    /**
     * This method can be called on-demand to return raw metric data.
     */
    protected abstract List<Metric> getMetrics();
}

