/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.util.Coerce;
import lombok.Builder;
import lombok.Value;

import java.util.Map;

import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MQTT_TOPIC_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MQTT_TOPIC_CONFIG_PARSE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.PUBSUB_PUBLISH_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.PUBSUB_PUBLISH_CONFIG_PARSE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.TELEMETRY_PUBLISH_INTERVAL_CONFIG_PARSE_ERROR_LOG;

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
     * @param pojo  POJO Topics object.
     * @param logger Greengrass logger.
     * @return  the Nucleus Emitter configuration.
     */
    public static NucleusEmitterConfiguration fromPojo(Map<String, Object> pojo, Logger logger) {
        if (pojo.isEmpty()) {
            return null;
        }
        long telemetryPublishIntervalMs = DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS;
        boolean pubsubPublish = true;
        String mqttTopic = "";
        for (Map.Entry<String, Object> entry : pojo.entrySet()) {
            switch (entry.getKey()) {
                case PUBSUB_PUBLISH_CONFIG_NAME:
                    if (entry.getValue() instanceof Boolean) {
                        pubsubPublish = Coerce.toBoolean(entry.getValue());
                        break;
                    } else {
                        logger.error(PUBSUB_PUBLISH_CONFIG_PARSE_ERROR_LOG, entry.getValue());
                        return null;
                    }
                case TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME:
                    if (entry.getValue() instanceof Number) {
                        telemetryPublishIntervalMs = Coerce.toLong(entry.getValue());
                        break;
                    } else {
                        logger.error(TELEMETRY_PUBLISH_INTERVAL_CONFIG_PARSE_ERROR_LOG, entry.getValue());
                        return null;
                    }
                case MQTT_TOPIC_CONFIG_NAME:
                    if (entry.getValue() instanceof String) {
                        mqttTopic = Coerce.toString(entry.getValue());
                        break;
                    } else {
                        logger.error(MQTT_TOPIC_CONFIG_PARSE_ERROR_LOG, entry.getValue());
                        return null;
                    }
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