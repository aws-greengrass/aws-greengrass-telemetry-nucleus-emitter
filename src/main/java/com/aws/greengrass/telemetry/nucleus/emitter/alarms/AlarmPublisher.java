/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter.alarms;

import com.aws.greengrass.telemetry.impl.Metric;
import com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterConfiguration;
import com.aws.greengrass.telemetry.nucleus.emitter.publisher.MqttPublisher;
import com.aws.greengrass.telemetry.nucleus.emitter.publisher.PubSubPublisher;
import com.aws.greengrass.util.Utils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Builder;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_TELEMETRY_PUBSUB_TOPIC;

@Builder
public class AlarmPublisher {

    private final AtomicReference<NucleusEmitterConfiguration> config;
    private final MqttPublisher mqttPublisher;
    private final PubSubPublisher pubSubPublisher;
    private final ObjectMapper mapper;

    public void publish(List<Metric> metrics){
        String message;
        try {
            message = mapper.writeValueAsString(metrics);
        } catch (JsonProcessingException e) {
            // TODO warn
            return;
        }
        // TODO atomicity on config
        if (false) { // TODO config.get().isPubsubPublish(), but for alarms
            pubSubPublisher.publishMessage(message, DEFAULT_TELEMETRY_PUBSUB_TOPIC);
        }
        String alertsMqttTopic = config.get().getAlertsMqttTopic();
        if (!Utils.isEmpty(alertsMqttTopic)) {
            mqttPublisher.publishMessage(message, alertsMqttTopic);
        }
    }
}
