/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.TreeMap;

import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.CONFIG_INVALID_OPTION_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_TELEMETRY_PUBSUB_TOPIC;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MQTT_TOPIC_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MQTT_TOPIC_CONFIG_PARSE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.PUBSUB_PUBLISH_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.PUBSUB_PUBLISH_CONFIG_PARSE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.TELEMETRY_PUBLISH_INTERVAL_CONFIG_PARSE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterConfiguration.fromPojo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class NucleusEmitterConfigurationTest extends GGServiceTestUtil {

    @Mock
    Logger logger;

    private final NucleusEmitterConfiguration defaultConfiguration = NucleusEmitterConfiguration.builder().build();

    @Test
    void GIVEN_valid_config_options_THEN_parses_correctly() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(MQTT_TOPIC_CONFIG_NAME,"");
        pojo.put(PUBSUB_PUBLISH_CONFIG_NAME, true);
        pojo.put(TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME, DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS);
        NucleusEmitterConfiguration generatedConfiguration = fromPojo(pojo, logger);
        assertEquals(defaultConfiguration, generatedConfiguration);
    }

    @Test
    void GIVEN_valid_string_config_options_THEN_parses_correctly() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(MQTT_TOPIC_CONFIG_NAME,"");
        pojo.put(PUBSUB_PUBLISH_CONFIG_NAME, "true");
        pojo.put(TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME, "60000");
        NucleusEmitterConfiguration generatedConfiguration = fromPojo(pojo, logger);
        assertEquals(defaultConfiguration, generatedConfiguration);
    }

    @Test
    void GIVEN_invalid_pubSubPublish_option_THEN_fails() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(MQTT_TOPIC_CONFIG_NAME,"");
        pojo.put(PUBSUB_PUBLISH_CONFIG_NAME, "garbage");
        pojo.put(TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME, DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS);
        NucleusEmitterConfiguration generatedConfiguration = fromPojo(pojo, logger);
        assertNull(generatedConfiguration);
        verify(logger).error(PUBSUB_PUBLISH_CONFIG_PARSE_ERROR_LOG, "garbage");
    }

    @Test
    void GIVEN_invalid_mqttTopic_option_THEN_fails() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(MQTT_TOPIC_CONFIG_NAME,4545);
        pojo.put(PUBSUB_PUBLISH_CONFIG_NAME, DEFAULT_TELEMETRY_PUBSUB_TOPIC);
        pojo.put(TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME, DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS);
        NucleusEmitterConfiguration generatedConfiguration = fromPojo(pojo, logger);
        assertNull(generatedConfiguration);
        verify(logger).error(MQTT_TOPIC_CONFIG_PARSE_ERROR_LOG, 4545);
    }

    @Test
    void GIVEN_invalid_telemetryPublishIntervalMs_option_THEN_fails() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(MQTT_TOPIC_CONFIG_NAME,"");
        pojo.put(PUBSUB_PUBLISH_CONFIG_NAME, true);
        pojo.put(TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME, "garbage");
        NucleusEmitterConfiguration generatedConfiguration = fromPojo(pojo, logger);
        assertNull(generatedConfiguration);
        verify(logger).error(TELEMETRY_PUBLISH_INTERVAL_CONFIG_PARSE_ERROR_LOG, "garbage");
    }

    @Test
    void GIVEN_null_config_options_THEN_fails() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME, null);
        NucleusEmitterConfiguration generatedConfiguration = fromPojo(pojo, logger);
        assertNull(generatedConfiguration);
        verify(logger).error(TELEMETRY_PUBLISH_INTERVAL_CONFIG_PARSE_ERROR_LOG, (Object) null);

        pojo = new TreeMap<>();
        pojo.put(MQTT_TOPIC_CONFIG_NAME,null);
        generatedConfiguration = fromPojo(pojo, logger);
        assertNull(generatedConfiguration);
        verify(logger).error(MQTT_TOPIC_CONFIG_PARSE_ERROR_LOG, (Object) null);

        pojo = new TreeMap<>();
        pojo.put(PUBSUB_PUBLISH_CONFIG_NAME, null);
        generatedConfiguration = fromPojo(pojo, logger);
        assertNull(generatedConfiguration);
        verify(logger).error(PUBSUB_PUBLISH_CONFIG_PARSE_ERROR_LOG, (Object) null);
    }

    @Test
    void GIVEN_invalid_config_option_THEN_fails() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put("garbage", "garbage");
        NucleusEmitterConfiguration generatedConfiguration = fromPojo(pojo, logger);
        assertNull(generatedConfiguration);
        verify(logger).error(CONFIG_INVALID_OPTION_ERROR_LOG, "garbage");
    }

    @Test
    void GIVEN_empty_config_THEN_fails() {
        Map<String, Object> pojo = new TreeMap<>();
        NucleusEmitterConfiguration generatedConfiguration = fromPojo(pojo, logger);
        assertNull(generatedConfiguration);
    }
}
