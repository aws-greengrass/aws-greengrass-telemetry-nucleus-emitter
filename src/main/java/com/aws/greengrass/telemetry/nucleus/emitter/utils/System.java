/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter.utils;

import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;

public final class System {
    public static final SystemInfo SYSTEM_INFO = new SystemInfo();
    public static final CentralProcessor CPU = SYSTEM_INFO.getHardware().getProcessor();
}
