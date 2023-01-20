/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter.alarms;

import javax.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class AlarmTask {

    private static final long TIME_TO_EVALUATE = 0L;

    private final Map<String, Monitor> monitors = new ConcurrentHashMap<>();
    private final Map<String, Long> minutesSinceEvaluation = new ConcurrentHashMap<>();
    private final Object lock = new Object();
    private ScheduledFuture<?> task;

    private final ScheduledExecutorService ses;
    private final AlarmPublisher alarmPublisher;


    @Inject
    public AlarmTask(ScheduledExecutorService ses, AlarmPublisher alarmPublisher) {
        this.ses = ses;
        this.alarmPublisher = alarmPublisher;
    }

    private void run() {
        monitors.forEach((name, monitor) -> {
            Threshold threshold = monitor.getThreshold();
            if (threshold == null) {
                // config was likely removed, do nothing
                return;
            }

            long minutes = minutesSinceEvaluation.compute(name, (k, mins) -> {
                if (mins == null) {
                    return TIME_TO_EVALUATE;
                }
                if (mins >= threshold.getPeriodTimeUnit().toMinutes(threshold.getPeriod())) {
                    return TIME_TO_EVALUATE;
                }
                // not time yet
                return mins + 1;
            });

            if (minutes == TIME_TO_EVALUATE) {
                if (monitor.evaluate() == Monitor.State.ALARM) {
                    alarmPublisher.publish(monitor.getDatapoints());
                }
            }
        });
    }

    public void start() {
        synchronized (lock) {
            stop();
            task = ses.scheduleAtFixedRate(this::run, 0, 1, TimeUnit.MINUTES);
        }
    }

    public void stop() {
        synchronized (lock) {
            if (task != null) {
                task.cancel(false);
            }
        }
    }

    public void register(String name, Monitor monitor) {
        monitors.putIfAbsent(name, monitor);
    }

    public void deregister(String name) {
        monitors.remove(name);
        minutesSinceEvaluation.remove(name);
    }
}
