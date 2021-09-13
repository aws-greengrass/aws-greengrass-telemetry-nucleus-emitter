/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter;

public class Constants {

    public static final String AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER = "aws.greengrass.telemetry.NucleusEmitter";

    public static final String PUBSUB_PUBLISH_CONFIG_NAME = "pubSubPublish";
    public static final String MQTT_PUBLISH_CONFIG_NAME = "mqttPublish";
    public static final String MQTT_TOPIC_CONFIG_NAME = "mqttTopic";
    public static final String TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME = "telemetryPublishIntervalMs";

    public static final String DEFAULT_TELEMETRY_PUBSUB_TOPIC = "$local/greengrass/telemetry";
    public static final String DEFAULT_TELEMETRY_MQTT_TOPIC = "greengrass/nucleus/telemetry";

    //60 seconds by default (~60MB/month of raw data)
    public static final int DEFAULT_TELEMETRY_PUBLISH_INTERVAL = 60_000;
    //500ms minimum (~7GB/month of raw data)
    public static final int MIN_TELEMETRY_PUBLISH_INTERVAL = 500;
    public static final String PUBSUB_PUBLISH_SUCCESS_LOG = "Published local pub/sub message on topic "
            + "'$local/greengrass/telemetry'";
    public static final String PUBSUB_PUBLISH_FAILURE_LOG = "Failed to publish local pub/sub message on topic "
                    + "'$local/greengrass/telemetry'";
    public static final String MQTT_PUBLISH_SUCCESS_LOG = "Published MQTT message on topic '{}'";
    public static final String MQTT_PUBLISH_FAILURE_LOG = "Failed to publish MQTT message on topic '{}'";
    public static final String INVALID_PUBLISH_THRESHOLD_LOG = "Publish interval should not be smaller than 500ms. "
            + "Using minimum of 500ms.";
    public static final String STARTUP_CONFIGURATION_LOG = "Starting telemetry emission with pubsubPublish:{}, "
            + "mqttPublish:{}, pubsubTopic:{}, mqttTopic:{}, telemetryPublishIntervalMs:{}";
    public static final String JSON_PARSE_ERROR_LOG = "Error while parsing metrics JSON";

}
