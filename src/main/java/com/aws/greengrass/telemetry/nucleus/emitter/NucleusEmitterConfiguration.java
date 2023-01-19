/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.util.Coerce;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.Value;
import org.apache.commons.lang3.BooleanUtils;

import java.util.Map;

import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.ALARMS_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.ALERTS_MQTT_TOPIC_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.ALERTS_MQTT_TOPIC_CONFIG_PARSE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.CONFIG_INVALID_OPTION_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.INVALID_PUBLISH_THRESHOLD_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MIN_TELEMETRY_PUBLISH_INTERVAL_MS;
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
    String alertsMqttTopic = "";
    @Builder.Default
    long telemetryPublishIntervalMs = DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS;

    Alarm cpuAlarm;
    Alarm memoryAlarm;
    Alarm diskAlarm;

    /**
     * Get the Nucleus Emitter configuration from the POJO map.
     *
     * @param pojo   POJO Topics object.
     * @param logger Greengrass logger.
     * @return the Nucleus Emitter configuration, or null if the POJO map is invalid.
     */
    public static NucleusEmitterConfiguration fromPojo(Map<String, Object> pojo, Logger logger) {
        if (pojo.isEmpty()) {
            return null;
        }
        long telemetryPublishIntervalMs = DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS;
        boolean pubsubPublish = true;
        String mqttTopic = "";
        String alertsMqttTopic = "";
        Alarms alarms = null;
        for (Map.Entry<String, Object> entry : pojo.entrySet()) {
            switch (entry.getKey()) {
                case PUBSUB_PUBLISH_CONFIG_NAME:
                    if (entry.getValue() instanceof Boolean || entry.getValue() instanceof String) {
                        //BooleanUtils.toBooleanObject will return null if invalid
                        Boolean parsedBoolean = BooleanUtils.toBooleanObject(entry.getValue().toString());
                        if (parsedBoolean == null) { //If value is invalid
                            logger.error(PUBSUB_PUBLISH_CONFIG_PARSE_ERROR_LOG, entry.getValue());
                            return null;
                        }
                        pubsubPublish = parsedBoolean;
                        break;
                    } else {
                        logger.error(PUBSUB_PUBLISH_CONFIG_PARSE_ERROR_LOG, entry.getValue());
                        return null;
                    }
                case TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME:
                    if (entry.getValue() instanceof Number || entry.getValue() instanceof String) {
                        telemetryPublishIntervalMs = Coerce.toLong(entry.getValue());
                        if (telemetryPublishIntervalMs == 0L) { //If value is 0 or non-numeric String
                            logger.error(TELEMETRY_PUBLISH_INTERVAL_CONFIG_PARSE_ERROR_LOG, entry.getValue());
                            return null;
                        }
                        // If the new requested publish interval is below the minimum, use the minimum
                        if (telemetryPublishIntervalMs < MIN_TELEMETRY_PUBLISH_INTERVAL_MS) {
                            logger.warn(INVALID_PUBLISH_THRESHOLD_LOG, MIN_TELEMETRY_PUBLISH_INTERVAL_MS,
                                    MIN_TELEMETRY_PUBLISH_INTERVAL_MS);
                            telemetryPublishIntervalMs = MIN_TELEMETRY_PUBLISH_INTERVAL_MS;
                        }
                        break;
                    } else { //If not a Number or String
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
                case ALERTS_MQTT_TOPIC_CONFIG_NAME:
                    if (entry.getValue() instanceof String) {
                        alertsMqttTopic = Coerce.toString(entry.getValue());
                        break;
                    } else {
                        logger.error(ALERTS_MQTT_TOPIC_CONFIG_PARSE_ERROR_LOG, entry.getValue());
                        return null;
                    }
                case ALARMS_CONFIG_NAME:
                    try {
                        alarms = NucleusEmitter.jsonMapper.convertValue(entry.getValue(), Alarms.class);
                        break;
                    } catch (Exception e) {
                        logger.error("Unable to map alarm configuration", e);
                        return null;
                    }
                default:
                    logger.error(CONFIG_INVALID_OPTION_ERROR_LOG, entry.getKey());
                    return null;
            }
        }

        return NucleusEmitterConfiguration.builder()
                .pubsubPublish(pubsubPublish)
                .mqttTopic(mqttTopic)
                .alertsMqttTopic(alertsMqttTopic)
                .cpuAlarm(alarms == null ? null : alarms.getCpu())
                .memoryAlarm(alarms == null ? null : alarms.getMemory())
                .diskAlarm(alarms == null ? null : alarms.getDisk())
                .telemetryPublishIntervalMs(telemetryPublishIntervalMs)
                .build();
    }

    @Getter
    @Setter
    @EqualsAndHashCode
    public static class Alarms {
        Alarm cpu;
        Alarm memory;
        Alarm disk;
    }

    @Getter
    @Setter
    @EqualsAndHashCode
    public static class Alarm {
        String condition;
        double value;
        long period;
        String periodUnit;
        int datapoints;
        int evaluationPeriod;
    }
}
