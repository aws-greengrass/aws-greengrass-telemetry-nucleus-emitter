/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.util.Coerce;
import com.aws.greengrass.util.Utils;
import lombok.Builder;
import lombok.Value;
import org.apache.commons.lang3.BooleanUtils;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.CONFIG_INVALID_OPTION_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_METRICS_LEVEL;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_OUTPUT_DIRECTORY;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_OUTPUT_MODE;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_TELEMETRY_PUBSUB_TOPIC;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DETAILED_METRICS_LEVEL;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.EXCLUDE_INTERFACES_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.EXCLUDE_MOUNTS_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.METRICS_LEVEL_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.METRICS_LEVEL_CONFIG_PARSE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MQTT_TOPIC_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MQTT_TOPIC_CONFIG_PARSE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.OUTPUT_DIRECTORY_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.OUTPUT_MODE_BOTH;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.OUTPUT_MODE_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.OUTPUT_MODE_CONFIG_PARSE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.OUTPUT_MODE_EMF;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.OUTPUT_MODE_IPC;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.PUBSUB_PUBLISH_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.PUBSUB_PUBLISH_CONFIG_PARSE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.PUBSUB_TOPIC_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.PUBSUB_TOPIC_CONFIG_PARSE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.TELEMETRY_PUBLISH_INTERVAL_CONFIG_PARSE_ERROR_LOG;

@Value
@Builder(toBuilder = true)
public class NucleusEmitterConfiguration {

    //Configurable options
    //Only local pub/sub is enabled by default
    @Builder.Default
    boolean pubsubPublish = true;
    @Builder.Default
    String pubsubTopic = DEFAULT_TELEMETRY_PUBSUB_TOPIC;

    @Builder.Default
    String mqttTopic = "";
    @Builder.Default
    long telemetryPublishIntervalMs = DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS;

    @Builder.Default
    String metricsLevel = DEFAULT_METRICS_LEVEL;
    @Builder.Default
    String outputMode = DEFAULT_OUTPUT_MODE;
    @Builder.Default
    String outputDirectory = DEFAULT_OUTPUT_DIRECTORY;
    @Builder.Default
    List<String> excludeMounts = Collections.emptyList();
    @Builder.Default
    List<String> excludeInterfaces = Collections.emptyList();

    public boolean isDetailedMetrics() {
        return DETAILED_METRICS_LEVEL.equals(metricsLevel);
    }

    public boolean isEmfEnabled() {
        return OUTPUT_MODE_EMF.equals(outputMode) || OUTPUT_MODE_BOTH.equals(outputMode);
    }

    /**
     * Get the Nucleus Emitter configuration from the POJO map.
     * @param pojo  POJO Topics object.
     * @param logger Greengrass logger.
     * @return the Nucleus Emitter configuration.
     */
    public static NucleusEmitterConfiguration fromPojo(Map<String, Object> pojo, Logger logger) {
        if (pojo.isEmpty()) {
            return null;
        }
        NucleusEmitterConfigurationBuilder config = NucleusEmitterConfiguration.builder();

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
                        config.pubsubPublish(parsedBoolean);
                        break;
                    } else {
                        logger.error(PUBSUB_PUBLISH_CONFIG_PARSE_ERROR_LOG, entry.getValue());
                        return null;
                    }
                case TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME:
                    if (entry.getValue() instanceof Number || entry.getValue() instanceof String) {
                        long telemetryPublishIntervalMs = Coerce.toLong(entry.getValue());
                        if (telemetryPublishIntervalMs == 0L) { //If value is 0 or non-numeric String
                            logger.error(TELEMETRY_PUBLISH_INTERVAL_CONFIG_PARSE_ERROR_LOG, entry.getValue());
                            return null;
                        }
                        config.telemetryPublishIntervalMs(telemetryPublishIntervalMs);
                        break;
                    } else { //If not a Number or String
                        logger.error(TELEMETRY_PUBLISH_INTERVAL_CONFIG_PARSE_ERROR_LOG, entry.getValue());
                        return null;
                    }
                case MQTT_TOPIC_CONFIG_NAME:
                    if (entry.getValue() instanceof String) {
                        config.mqttTopic(Coerce.toString(entry.getValue()));
                        break;
                    } else {
                        logger.error(MQTT_TOPIC_CONFIG_PARSE_ERROR_LOG, entry.getValue());
                        return null;
                    }
                case PUBSUB_TOPIC_CONFIG_NAME:
                    if (entry.getValue() instanceof String) {
                        config.pubsubTopic(Coerce.toString(entry.getValue()));
                        break;
                    } else {
                        logger.error(PUBSUB_TOPIC_CONFIG_PARSE_ERROR_LOG, entry.getValue());
                        return null;
                    }
                case METRICS_LEVEL_CONFIG_NAME:
                    String mlVal = Coerce.toString(entry.getValue())
                            .toLowerCase(Locale.ROOT);
                    if (mlVal.isEmpty()) {
                        break;
                    }
                    if (DEFAULT_METRICS_LEVEL.equals(mlVal)
                            || DETAILED_METRICS_LEVEL.equals(mlVal)) {
                        config.metricsLevel(mlVal);
                        break;
                    }
                    logger.error(METRICS_LEVEL_CONFIG_PARSE_ERROR_LOG, entry.getValue());
                    return null;
                case OUTPUT_MODE_CONFIG_NAME:
                    String omVal = Coerce.toString(entry.getValue())
                            .toLowerCase(Locale.ROOT);
                    if (omVal.isEmpty()) {
                        break;
                    }
                    if (OUTPUT_MODE_IPC.equals(omVal) || OUTPUT_MODE_EMF.equals(omVal)
                            || OUTPUT_MODE_BOTH.equals(omVal)) {
                        config.outputMode(omVal);
                        break;
                    }
                    logger.error(OUTPUT_MODE_CONFIG_PARSE_ERROR_LOG, entry.getValue());
                    return null;
                case OUTPUT_DIRECTORY_CONFIG_NAME:
                    String dirVal = Coerce.toString(entry.getValue());
                    if (!Utils.isEmpty(dirVal)) {
                        config.outputDirectory(dirVal);
                    }
                    break;
                case EXCLUDE_MOUNTS_CONFIG_NAME:
                    config.excludeMounts(Coerce.toStringList(entry.getValue()));
                    break;
                case EXCLUDE_INTERFACES_CONFIG_NAME:
                    config.excludeInterfaces(Coerce.toStringList(entry.getValue()));
                    break;
                default:
                    logger.error(CONFIG_INVALID_OPTION_ERROR_LOG, entry.getKey());
                    return null;
            }
        }

        return config.build();
    }

}
