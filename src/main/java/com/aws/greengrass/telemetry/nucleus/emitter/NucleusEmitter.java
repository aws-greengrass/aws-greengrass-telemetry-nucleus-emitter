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
import com.aws.greengrass.telemetry.nucleus.emitter.metrics.KernelMetricsEmitter;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.CONFIG_UPDATE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_TELEMETRY_PUBSUB_TOPIC;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.INVALID_PUBLISH_THRESHOLD_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.JSON_PARSE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MIN_TELEMETRY_PUBLISH_INTERVAL_MS;
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
    private ScheduledFuture<?> telemetryAlertPublishFuture;

    @Getter(AccessLevel.PACKAGE) // Needed for unit tests.
    private final AtomicReference<NucleusEmitterConfiguration> currentConfiguration =
            new AtomicReference<>(NucleusEmitterConfiguration.builder().build());

    private static final ObjectMapper jsonMapper = SerializerFactory.getFailSafeJsonObjectMapper();

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
        // TODO remove, hack to support unit test
        configureMonitors();
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
        // TODO if alarms topic changed...
        boolean telemetryPublishIntervalMsChanged = configuration.getTelemetryPublishIntervalMs()
                != newConfiguration.getTelemetryPublishIntervalMs();
        // TODO if alarm config changes, restart alarm

        if (!pubSubPublishChanged && !mqttTopicChanged && !telemetryPublishIntervalMsChanged) {
            return;
        }

        //If the new requested publish interval is below the minimum, use the minimum
        if (newConfiguration.getTelemetryPublishIntervalMs() < MIN_TELEMETRY_PUBLISH_INTERVAL_MS) {
            logger.warn(INVALID_PUBLISH_THRESHOLD_LOG, MIN_TELEMETRY_PUBLISH_INTERVAL_MS,
                    MIN_TELEMETRY_PUBLISH_INTERVAL_MS);
            newConfiguration = NucleusEmitterConfiguration.builder()
                    .pubsubPublish(newConfiguration.isPubsubPublish())
                    .mqttTopic(newConfiguration.getMqttTopic())
                    .alertsMqttTopic(newConfiguration.getAlertsMqttTopic())
                    .telemetryPublishIntervalMs(MIN_TELEMETRY_PUBLISH_INTERVAL_MS)
                    .build();
        }
        
        currentConfiguration.set(newConfiguration);
        scheduleTelemetryPublish();
    }

    // TODO call this from config change
    private void configureMonitors() {
        // TODO configurable
        // TODO add other metrics
        // TODO move metric names to constants
        monitorsByMetricName.computeIfAbsent("CpuUsage", k ->
                Monitor.builder()
                        .ses(ses)
                        .datapoint(new CpuMetric(SystemMetricsEmitter.NAMESPACE))
                        .callback(this::handleMonitorState)
                        .threshold(Threshold.builder()
                                .condition(Threshold.Condition.GREATER)
                                .value(95)
                                .period(5)
                                .periodTimeUnit(TimeUnit.SECONDS)
                                .datapoints(1)
                                .evaluationPeriods(1)
                                .build())
                        .build());
    }

    private void handleMonitorState(Monitor.State state, List<Metric> datapoints) {
        // TODO revise message to include all metrics, or a summary?
        if (state == Monitor.State.ALARM) {
            // TODO check if this needs to be async
            publishTelemetryMessage(
                    false,
                    !Utils.isEmpty(currentConfiguration.get().getAlertsMqttTopic()) ,
                    currentConfiguration.get().getAlertsMqttTopic(),
                    datapoints.get(0)
            );
        }
    }

    @Override
    public void startup() {
        reportState(State.RUNNING);
        config.lookupTopics(CONFIGURATION_CONFIG_KEY).subscribe(subscribeToConfigChanges);
        scheduleTelemetryPublish();
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

    private void publishTelemetryMessage(boolean pubSubPublish, boolean mqttPublish, String mqttTopic, Metric telemetryMetric){
        String jsonString = null;
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

    protected void publishAlertTelemetry(boolean pubSubPublish, boolean mqttPublish, String mqttTopic){
        List<Metric> metrics = Stream.of(sme.getMetrics(), kme.getMetrics())
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        for (Metric m: metrics) {
            if(m.getName().equals("SystemMemUsagePercentage")){
                double systemMemUsagePercentage = (double)m.getValue();
                if(systemMemUsagePercentage > 95){
                    publishTelemetryMessage(pubSubPublish, mqttPublish, mqttTopic, m);
                }
            }
            if(m.getName().equals("SystemDiskUsagePercentage")){
                double systemDiskUsagePercentage = (double)m.getValue();
                if(systemDiskUsagePercentage > 95){
                    publishTelemetryMessage(pubSubPublish, mqttPublish, mqttTopic, m);
                }
            }
        }
    }

    private void scheduleTelemetryPublish() {
        final NucleusEmitterConfiguration configuration = currentConfiguration.get();
        final boolean newPubPublish = configuration.isPubsubPublish();
        final String newMqttTopic = configuration.getMqttTopic();
        final String alertsMqttTopic = configuration.getAlertsMqttTopic();
        final long newTelemetryPublishIntervalMs = configuration.getTelemetryPublishIntervalMs();
        //Start publish thread
        synchronized (publishTaskLock) {
            if (telemetryPublishFuture != null) {
                logger.debug(TELEMETRY_PUBLISH_STOPPING);
                cancelPublishTasks(false);
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
            telemetryAlertPublishFuture = ses.scheduleAtFixedRate(
                    () -> publishAlertTelemetry(false, !Utils.isEmpty(alertsMqttTopic), alertsMqttTopic), 0,
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
        cancelPublishTasks(true);
        monitorsByMetricName.values().forEach(Monitor::stop);
    }

    private void cancelPublishTasks(boolean interrupt) {
        synchronized (publishTaskLock) {
            if (telemetryPublishFuture != null) {
                telemetryPublishFuture.cancel(interrupt);
            }
            if (telemetryAlertPublishFuture != null) {
                telemetryAlertPublishFuture.cancel(interrupt);
            }
        }
    }
}
