/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter;

public class Constants {

    public static final String AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER = "aws.greengrass.telemetry.NucleusEmitter";

    public static final String PUBSUB_PUBLISH_CONFIG_NAME = "pubSubPublish";
    public static final String PUBSUB_TOPIC_CONFIG_NAME = "pubSubTopic";
    public static final String MQTT_TOPIC_CONFIG_NAME = "mqttTopic";
    public static final String TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME = "telemetryPublishIntervalMs";
    public static final String DEFAULT_TELEMETRY_PUBSUB_TOPIC = "$local/greengrass/telemetry";

    //60 seconds by default (~60MB/month of raw data)
    public static final long DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS = 60_000;
    //500ms minimum (~7GB/month of raw data)
    public static final long MIN_TELEMETRY_PUBLISH_INTERVAL_MS = 500;

    public static final String PUBSUB_PUBLISH_SUCCESS_LOG = "Published local pub/sub message on topic "
            + "'{}'";
    public static final String PUBSUB_PUBLISH_FAILURE_LOG = "Failed to publish local pub/sub message on topic "
                    + "'{}'";
    public static final String TELEMETRY_PUBLISH_SCHEDULED = "Scheduling telemetry publish";
    public static final String TELEMETRY_PUBLISH_STOPPING = "Stopping telemetry publish";
    public static final String PUBSUB_PUBLISH_STARTING = "Starting local pub/sub publishing";
    public static final String MQTT_PUBLISH_SUCCESS_LOG = "Published MQTT message on topic '{}'";
    public static final String MQTT_PUBLISH_FAILURE_LOG = "Failed to publish MQTT message on topic '{}'";
    public static final String MQTT_PUBLISH_STARTING = "Starting MQTT publishing";
    public static final String INVALID_PUBLISH_THRESHOLD_LOG = "Publish interval should not be smaller than 500ms. "
            + "Using minimum of 500ms";
    public static final String STARTUP_CONFIGURATION_LOG = "Starting telemetry emission with pubSubPublish:{}, "
            + "pubSubTopic:{}, mqttTopic:{}, telemetryPublishIntervalMs:{}";
    public static final String JSON_PARSE_ERROR_LOG = "Error while parsing metrics JSON";
    public static final String CONFIG_UPDATE_ERROR_LOG = "Failed to apply configuration update. Please check your "
            + "specified configuration";
    public static final String CONFIG_INVALID_OPTION_ERROR_LOG = "Invalid config option {}. Please check your specified"
            + " configuration.";
    public static final String PUBSUB_PUBLISH_CONFIG_PARSE_ERROR_LOG = "Could not parse the pubSubPublish config option"
            + " {}. Please make sure this is set to a valid boolean value";

    public static final String PUBSUB_TOPIC_CONFIG_PARSE_ERROR_LOG = "Could not parse the pubSubTopic config option {}."
            + " Please make sure this is set to a valid topic string value";
    public static final String MQTT_TOPIC_CONFIG_PARSE_ERROR_LOG = "Could not parse the mqttTopic config option {}."
            + " Please make sure this is set to a valid topic string value";
    public static final String TELEMETRY_PUBLISH_INTERVAL_CONFIG_PARSE_ERROR_LOG = "Could not parse the "
            + "telemetryPublishIntervalMs config option {}. Please make sure this is set to a valid non-zero long "
            + "value";
}
