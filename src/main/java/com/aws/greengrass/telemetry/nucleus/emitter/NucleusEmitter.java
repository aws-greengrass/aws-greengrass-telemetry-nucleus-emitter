/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter;

import com.aws.greengrass.config.ChildChanged;
import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.ImplementsService;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.PluginService;
import com.aws.greengrass.telemetry.impl.Metric;
import com.aws.greengrass.telemetry.nucleus.emitter.alarms.Monitor;
import com.aws.greengrass.telemetry.nucleus.emitter.alarms.Threshold;
import com.aws.greengrass.telemetry.nucleus.emitter.metrics.CpuMetric;
import com.aws.greengrass.telemetry.nucleus.emitter.metrics.DiskMetric;
import com.aws.greengrass.telemetry.nucleus.emitter.metrics.KernelMetricsEmitter;
import com.aws.greengrass.telemetry.nucleus.emitter.metrics.MemoryMetric;
import com.aws.greengrass.telemetry.nucleus.emitter.metrics.SystemMetricsEmitter;
import com.aws.greengrass.telemetry.nucleus.emitter.publisher.MqttPublisher;
import com.aws.greengrass.telemetry.nucleus.emitter.publisher.PubSubPublisher;
import com.aws.greengrass.util.SerializerFactory;
import com.aws.greengrass.util.Utils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AccessLevel;
import lombok.Getter;

import javax.inject.Inject;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.CONFIG_UPDATE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_TELEMETRY_PUBSUB_TOPIC;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.JSON_PARSE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MQTT_PUBLISH_STARTING;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.PUBSUB_PUBLISH_STARTING;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.STARTUP_CONFIGURATION_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.TELEMETRY_PUBLISH_SCHEDULED;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.TELEMETRY_PUBLISH_STOPPING;

@ImplementsService(name = AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER)
public class NucleusEmitter extends PluginService {

    private final ScheduledExecutorService ses;
    private final Object publishTaskLock = new Object();
    private ScheduledFuture<?> telemetryPublishFuture;

    @Getter(AccessLevel.PACKAGE) // Needed for unit tests.
    private final AtomicReference<NucleusEmitterConfiguration> currentConfiguration =
            new AtomicReference<>(NucleusEmitterConfiguration.builder().build());

    static final ObjectMapper jsonMapper = SerializerFactory.getFailSafeJsonObjectMapper();

    private final Map<String, Monitor> monitorsByMetricName = new HashMap<>();

    private final SystemMetricsEmitter sme;
    private final KernelMetricsEmitter kme;

    //Metric publishers
    private final PubSubPublisher pubSubPublisher;
    private final MqttPublisher mqttPublisher;

    private final ChildChanged subscribeToConfigChanges = (what, topic) ->
            handleConfiguration(this.config.lookupTopics(CONFIGURATION_CONFIG_KEY));

    /**
     *  Constructs a new NucleusEmitter to start publishing telemetry from the Nucleus.
     *
     * @param t                 {@link Topics}
     * @param sme               {@link SystemMetricsEmitter}
     * @param kme               {@link KernelMetricsEmitter}
     * @param pubSubPublisher   {@link PubSubPublisher}
     * @param mqttPublisher     {@link MqttPublisher}
     * @param ses               {@link ScheduledExecutorService}
     *
     */
    @Inject
    public NucleusEmitter(Topics t, SystemMetricsEmitter sme, KernelMetricsEmitter kme,
                          PubSubPublisher pubSubPublisher, MqttPublisher mqttPublisher,
                          ScheduledExecutorService ses) {
        super(t);
        this.sme = sme;
        this.kme = kme;
        this.pubSubPublisher = pubSubPublisher;
        this.mqttPublisher = mqttPublisher;
        this.ses = ses;
    }

    private void handleConfiguration(Topics configurationTopics) {
        NucleusEmitterConfiguration newConfiguration =
                NucleusEmitterConfiguration.fromPojo(configurationTopics.toPOJO(), logger);
        if (newConfiguration == null) {
            logger.error(CONFIG_UPDATE_ERROR_LOG);
            return;
        }
        NucleusEmitterConfiguration configuration = currentConfiguration.get();

        boolean pubSubPublishChanged = configuration.isPubsubPublish() != newConfiguration.isPubsubPublish();
        boolean mqttTopicChanged = !configuration.getMqttTopic().equals(newConfiguration.getMqttTopic());
        // TODO consistent naming
        boolean alarmsMqttTopicChanged = !configuration.getAlertsMqttTopic().equals(newConfiguration.getAlertsMqttTopic());
        boolean telemetryPublishIntervalMsChanged = configuration.getTelemetryPublishIntervalMs()
                != newConfiguration.getTelemetryPublishIntervalMs();
        boolean cpuAlarmChanged = !Objects.equals(configuration.getCpuAlarm(), newConfiguration.getCpuAlarm());
        boolean memoryAlarmChanged = !Objects.equals(configuration.getMemoryAlarm(), newConfiguration.getMemoryAlarm());
        boolean diskAlarmChanged = !Objects.equals(configuration.getDiskAlarm(), newConfiguration.getDiskAlarm());

        if (!pubSubPublishChanged && !mqttTopicChanged
                && !telemetryPublishIntervalMsChanged && !alarmsMqttTopicChanged
                && !cpuAlarmChanged && !memoryAlarmChanged && !diskAlarmChanged) {
            return;
        }

        if (cpuAlarmChanged) {
            restartMonitorWithConfig(
                    CpuMetric.NAME,
                    new CpuMetric(SystemMetricsEmitter.NAMESPACE),
                    newConfiguration.getCpuAlarm());
        }
        if (memoryAlarmChanged) {
            restartMonitorWithConfig(
                    MemoryMetric.NAME,
                    new MemoryMetric(SystemMetricsEmitter.NAMESPACE),
                    newConfiguration.getMemoryAlarm());
        }
        if (diskAlarmChanged) {
            restartMonitorWithConfig(
                    DiskMetric.NAME,
                    new DiskMetric(SystemMetricsEmitter.NAMESPACE),
                    newConfiguration.getDiskAlarm());
        }

        currentConfiguration.set(newConfiguration);
        // TODO don't do this if only alarms changed
        scheduleTelemetryPublish();
    }

    private void restartMonitorWithConfig(String name, Supplier<Metric> datapoint, NucleusEmitterConfiguration.Alarm conf) {
        monitorsByMetricName.compute(name, (n, m) -> {
            if (m != null) {
                m.stop();
            }
            return Monitor.builder()
                    .ses(ses)
                    .datapoint(datapoint)
                    .callback(this::handleMonitorState)
                    // TODO validation
                    .threshold(Threshold.builder()
                            .condition(Threshold.Condition.fromExpr(conf.getCondition()).get())
                            .value(conf.getValue())
                            .period(conf.getPeriod())
                            .periodTimeUnit(TimeUnit.valueOf(conf.getPeriodUnit()))
                            .datapoints(conf.getDatapoints())
                            .evaluationPeriods(conf.getEvaluationPeriod())
                            .build())
                    .build();
        }).start();
    }

    private void handleMonitorState(Monitor.State state, List<Metric> datapoints) {
        if (state == Monitor.State.ALARM) {
            // TODO check if this needs to be async
            publishAlarm(
                    false,
                    !Utils.isEmpty(currentConfiguration.get().getAlertsMqttTopic()),
                    currentConfiguration.get().getAlertsMqttTopic(),
                    // TODO revise message to include all metrics, or a summary?
                    datapoints.get(0)
            );
        }
    }

    @Override
    public void startup() {
        reportState(State.RUNNING);
        config.lookupTopics(CONFIGURATION_CONFIG_KEY).subscribe(subscribeToConfigChanges);
        scheduleTelemetryPublish();
        // TODO if we're in alarm state, we get duplicate alarms from this and from config change
        monitorsByMetricName.values().forEach(Monitor::start);
    }

    private void publishTelemetry(boolean pubSubPublish, boolean mqttPublish, String mqttTopic) {
        String jsonString = retrieveMetricsJson(jsonMapper);
        if (pubSubPublish) {
            this.pubSubPublisher.publishMessage(jsonString, DEFAULT_TELEMETRY_PUBSUB_TOPIC);
        }
        if (mqttPublish) {
            this.mqttPublisher.publishMessage(jsonString, mqttTopic);
        }
    }

    private void publishAlarm(boolean pubSubPublish, boolean mqttPublish, String mqttTopic, Metric telemetryMetric){
        String jsonString;
        try {
            jsonString = jsonMapper.writeValueAsString(telemetryMetric);
        } catch (JsonProcessingException e) {
           return;
        }
        if (pubSubPublish) {
            this.pubSubPublisher.publishMessage(jsonString, DEFAULT_TELEMETRY_PUBSUB_TOPIC);
        }
        if (mqttPublish) {
            this.mqttPublisher.publishMessage(jsonString, mqttTopic);
        }
    }

    private void scheduleTelemetryPublish() {
        final NucleusEmitterConfiguration configuration = currentConfiguration.get();
        final boolean newPubPublish = configuration.isPubsubPublish();
        final String newMqttTopic = configuration.getMqttTopic();
        final long newTelemetryPublishIntervalMs = configuration.getTelemetryPublishIntervalMs();
        //Start publish thread
        synchronized (publishTaskLock) {
            if (telemetryPublishFuture != null) {
                logger.debug(TELEMETRY_PUBLISH_STOPPING);
                cancelPublishTask(false);
            }
            if (newPubPublish) {
                logger.debug(PUBSUB_PUBLISH_STARTING);
            }
            if (!Utils.isEmpty(newMqttTopic)) {
                logger.debug(MQTT_PUBLISH_STARTING);
            }
            logger.debug(TELEMETRY_PUBLISH_SCHEDULED);
            telemetryPublishFuture = ses.scheduleAtFixedRate(
                    () -> publishTelemetry(newPubPublish, !Utils.isEmpty(newMqttTopic), newMqttTopic), 0,
                    newTelemetryPublishIntervalMs, TimeUnit.MILLISECONDS);
        }

        logger.info(STARTUP_CONFIGURATION_LOG, newPubPublish,
                DEFAULT_TELEMETRY_PUBSUB_TOPIC, newMqttTopic, newTelemetryPublishIntervalMs);
    }

    protected String retrieveMetricsJson(ObjectMapper jsonMapper) {
        String jsonString = null;
        try {
            List<Metric> metrics = Stream.of(sme.getMetrics(), kme.getMetrics())
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
            jsonString = jsonMapper.writeValueAsString(metrics);
        } catch (JsonProcessingException e) {
            logger.error(JSON_PARSE_ERROR_LOG, e);
        }
        return jsonString;
    }

    @Override
    public void shutdown() {
        cancelPublishTask(true);
        monitorsByMetricName.values().forEach(Monitor::stop);
    }

    private void cancelPublishTask(boolean interrupt) {
        synchronized (publishTaskLock) {
            if (telemetryPublishFuture != null) {
                telemetryPublishFuture.cancel(interrupt);
            }
        }
    }
}
