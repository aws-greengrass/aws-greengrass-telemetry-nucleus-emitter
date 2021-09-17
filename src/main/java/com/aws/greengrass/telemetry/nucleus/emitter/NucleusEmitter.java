/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter;

import com.aws.greengrass.config.Topics;
import com.aws.greengrass.dependency.ImplementsService;
import com.aws.greengrass.dependency.State;
import com.aws.greengrass.ipc.AuthenticationHandler;
import com.aws.greengrass.lifecyclemanager.PluginService;
import com.aws.greengrass.telemetry.impl.Metric;
import com.aws.greengrass.telemetry.nucleus.emitter.metrics.KernelMetricsEmitter;
import com.aws.greengrass.telemetry.nucleus.emitter.metrics.SystemMetricsEmitter;
import com.aws.greengrass.telemetry.nucleus.emitter.publisher.MqttPublisher;
import com.aws.greengrass.telemetry.nucleus.emitter.publisher.PubSubPublisher;
import com.aws.greengrass.util.SerializerFactory;
import com.aws.greengrass.util.Utils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import lombok.AccessLevel;
import lombok.Getter;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_TELEMETRY_PUBSUB_TOPIC;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.INVALID_PUBLISH_THRESHOLD_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.JSON_PARSE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MIN_TELEMETRY_PUBLISH_INTERVAL_MS;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MQTT_PUBLISH_STARTING;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MQTT_PUBLISH_STOPPING;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.PUBSUB_PUBLISH_STARTING;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.PUBSUB_PUBLISH_STOPPING;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.STARTUP_CONFIGURATION_LOG;

@ImplementsService(name = AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER, autostart = true)
public class NucleusEmitter extends PluginService {

    private final ScheduledExecutorService ses;
    private final ExecutorService executorService;
    private final Object telemetryPubSubPublishInProgressLock = new Object();
    private final Object telemetryMqttPublishInProgressLock = new Object();
    private ScheduledFuture<?> telemetryPubSubPublishFuture;
    private ScheduledFuture<?> telemetryMqttPublishFuture;

    @Getter(AccessLevel.PACKAGE) // Needed for unit tests.
    private final AtomicReference<NucleusEmitterConfiguration> currentConfiguration =
            new AtomicReference<>(NucleusEmitterConfiguration.builder().build());

    private static final ObjectMapper jsonMapper = SerializerFactory.getFailSafeJsonObjectMapper();

    private final SystemMetricsEmitter sme;
    private final KernelMetricsEmitter kme;

    //RTT publishers
    private final PubSubPublisher pubSubPublisher;
    private final MqttPublisher mqttPublisher;


    /**
     *  Constructs a new NucleusEmitter to start publishing telemetry from the Nucleus.
     *
     * @param t                 {@link Topics}
     * @param sme               {@link SystemMetricsEmitter}
     * @param kme               {@link KernelMetricsEmitter}
     * @param pubSubPublisher   {@link PubSubPublisher}
     * @param mqttPublisher     {@link MqttPublisher}
     * @param ses               {@link ScheduledExecutorService}
     * @param executorService   {@link ExecutorService}
     *
     */
    @Inject
    public NucleusEmitter(Topics t, SystemMetricsEmitter sme, KernelMetricsEmitter kme,
                          PubSubPublisher pubSubPublisher, MqttPublisher mqttPublisher,
                          ScheduledExecutorService ses, ExecutorService executorService) {
        super(t);
        this.sme = sme;
        this.kme = kme;
        this.pubSubPublisher = pubSubPublisher;
        this.mqttPublisher = mqttPublisher;
        this.ses = ses;
        this.executorService = executorService;
    }

    @Override
    @SuppressFBWarnings(value = {"RV_RETURN_VALUE_IGNORED_BAD_PRACTICE"},
            justification = "postInject() return value does not need to be handled")
    public void postInject() {
        executorService.submit(() -> {
            Topics configurationTopics = config.lookupTopics(CONFIGURATION_CONFIG_KEY);
            configurationTopics.subscribe((why, newv) -> handleConfiguration(configurationTopics));
            handleConfiguration(configurationTopics);
        });
        super.postInject();

        // Does not happen for built-in/plugin services so doing explicitly
        AuthenticationHandler.registerAuthenticationToken(this);
    }

    private void handleConfiguration(Topics configurationTopics) {
        NucleusEmitterConfiguration newConfiguration =
                NucleusEmitterConfiguration.fromPojo(configurationTopics.toPOJO());
        NucleusEmitterConfiguration configuration = currentConfiguration.get();

        boolean pubSubPublishChanged = configuration.isPubsubPublish() != newConfiguration.isPubsubPublish();
        boolean mqttTopicChanged = !configuration.getMqttTopic().equals(newConfiguration.getMqttTopic());
        boolean telemetryPublishIntervalMsChanged = configuration.getTelemetryPublishIntervalMs()
                != newConfiguration.getTelemetryPublishIntervalMs();

        //If the new requested publish interval is below the minimum, use the minimum
        if (newConfiguration.getTelemetryPublishIntervalMs() < MIN_TELEMETRY_PUBLISH_INTERVAL_MS) {
            logger.warn(INVALID_PUBLISH_THRESHOLD_LOG, MIN_TELEMETRY_PUBLISH_INTERVAL_MS,
                    MIN_TELEMETRY_PUBLISH_INTERVAL_MS);
            newConfiguration = NucleusEmitterConfiguration.builder()
                    .pubsubPublish(newConfiguration.isPubsubPublish())
                    .mqttTopic(newConfiguration.getMqttTopic())
                    .telemetryPublishIntervalMs(MIN_TELEMETRY_PUBLISH_INTERVAL_MS)
                    .build();
        }

        currentConfiguration.set(newConfiguration);

        if (telemetryPublishIntervalMsChanged) {
            scheduleTelemetryPublish(true, true);
        } else if (pubSubPublishChanged || mqttTopicChanged) {
            scheduleTelemetryPublish(pubSubPublishChanged, mqttTopicChanged);
        }
    }

    @SuppressWarnings("UseSpecificCatch")
    @Override
    public void startup() {
        reportState(State.RUNNING);
        scheduleTelemetryPublish(true, true);
    }

    private void publishMqttTelemetry(String mqttTopic) {
        String jsonString = retrieveMetricsJson(jsonMapper);
        this.mqttPublisher.publishMessage(jsonString, mqttTopic);
    }

    private void publishPubSubTelemetry() {
        String jsonString = retrieveMetricsJson(jsonMapper);
        this.pubSubPublisher.publishMessage(jsonString, DEFAULT_TELEMETRY_PUBSUB_TOPIC);
    }

    private void scheduleTelemetryPublish(boolean pubSubPublishChanged, boolean mqttTopicChanged) {
        final NucleusEmitterConfiguration configuration = currentConfiguration.get();
        final boolean newPubPublish = configuration.isPubsubPublish();
        final String newMqttTopic = configuration.getMqttTopic();
        final long newTelemetryPublishIntervalMs = configuration.getTelemetryPublishIntervalMs();

        //Only change if either telemetryPublishIntervalMs or pubSubPublish is changed
        if (pubSubPublishChanged) {
            if (telemetryPubSubPublishFuture != null) {
                logger.warn(PUBSUB_PUBLISH_STOPPING);
                cancelJob(telemetryPubSubPublishFuture, telemetryPubSubPublishInProgressLock, false);
            }
            if (newPubPublish) {
                synchronized (telemetryPubSubPublishInProgressLock) {
                    logger.info(PUBSUB_PUBLISH_STARTING);
                    telemetryPubSubPublishFuture = ses.scheduleAtFixedRate(
                            this::publishPubSubTelemetry, 0,
                            newTelemetryPublishIntervalMs, TimeUnit.MILLISECONDS);
                }
            }
        }
        //Only change if either telemetryPublishIntervalMs or mqttTopic is changed
        if (mqttTopicChanged) {
            if (telemetryMqttPublishFuture != null) {
                logger.warn(MQTT_PUBLISH_STOPPING);
                cancelJob(telemetryMqttPublishFuture, telemetryMqttPublishInProgressLock, false);
            }

            if (!Utils.isEmpty(newMqttTopic)) {
                synchronized (telemetryMqttPublishInProgressLock) {
                    logger.info(MQTT_PUBLISH_STARTING);
                    telemetryMqttPublishFuture = ses.scheduleAtFixedRate(
                            () -> publishMqttTelemetry(newMqttTopic), 0,
                            newTelemetryPublishIntervalMs, TimeUnit.MILLISECONDS);
                }
            }
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
        cancelJob(telemetryPubSubPublishFuture, telemetryPubSubPublishInProgressLock, true);
        cancelJob(telemetryMqttPublishFuture, telemetryMqttPublishInProgressLock, true);
        ses.shutdown();
    }

    private void cancelJob(ScheduledFuture<?> future, Object lock, boolean immediately) {
        synchronized (lock) {
            if (future != null) {
                future.cancel(immediately);
            }
        }
    }
}
