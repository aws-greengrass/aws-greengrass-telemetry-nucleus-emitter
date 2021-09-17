/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter;

import com.aws.greengrass.util.Coerce;
import lombok.Builder;
import lombok.Value;

import java.util.Map;

import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MQTT_TOPIC_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.PUBSUB_PUBLISH_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME;

@Value
@Builder
public class NucleusEmitterConfiguration {

    //Configurable options
    //Only local pub/sub is enabled by default
    @Builder.Default
    boolean pubsubPublish = true;
    @Builder.Default
    String mqttTopic = "";
    @Builder.Default
    long telemetryPublishIntervalMs = DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS;

    /**
     * Get the Nucleus Emitter configuration from the POJO map.
     * @param pojo  POJO object.
     * @return  the Nucleus Emitter configuration.
     */
    public static NucleusEmitterConfiguration fromPojo(Map<String, Object> pojo) {
        long telemetryPublishIntervalMs = DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS;
        boolean pubsubPublish = true;
        String mqttTopic = "";
        for (Map.Entry<String, Object> entry : pojo.entrySet()) {
            switch (entry.getKey()) {
                case PUBSUB_PUBLISH_CONFIG_NAME:
                    pubsubPublish = Coerce.toBoolean(entry.getValue());
                    break;
                case TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME:
                    telemetryPublishIntervalMs = Coerce.toLong(entry.getValue());
                    break;
                case MQTT_TOPIC_CONFIG_NAME:
                    mqttTopic = Coerce.toString(entry.getValue());
                    break;
                default:
                    break;
            }
        }

        return NucleusEmitterConfiguration.builder()
                .pubsubPublish(pubsubPublish)
                .mqttTopic(mqttTopic)
                .telemetryPublishIntervalMs(telemetryPublishIntervalMs)
                .build();
    }
}