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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

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
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.TELEMETRY_PUBLISH_INTERVAL_CONFIG_PARSE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterConfiguration.fromPojo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
    void GIVEN_valid_nondefault_string_config_options_THEN_parses_correctly() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(PUBSUB_TOPIC_CONFIG_NAME,"pubsub");
        NucleusEmitterConfiguration generatedConfiguration = fromPojo(pojo, logger);
        assertEquals("pubsub", generatedConfiguration.getPubsubTopic());
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

    @Test
    void GIVEN_default_config_THEN_has_correct_defaults() {
        NucleusEmitterConfiguration config = NucleusEmitterConfiguration.builder().build();
        assertEquals(DEFAULT_METRICS_LEVEL, config.getMetricsLevel());
        assertEquals(DEFAULT_OUTPUT_MODE, config.getOutputMode());
        assertEquals(DEFAULT_OUTPUT_DIRECTORY, config.getOutputDirectory());
        assertEquals(Collections.emptyList(), config.getExcludeMounts());
        assertEquals(Collections.emptyList(), config.getExcludeInterfaces());
    }

    @Test
    void GIVEN_metricsLevel_basic_THEN_parses_correctly() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(METRICS_LEVEL_CONFIG_NAME, DEFAULT_METRICS_LEVEL);
        NucleusEmitterConfiguration config = fromPojo(pojo, logger);
        assertEquals(DEFAULT_METRICS_LEVEL, config.getMetricsLevel());
    }

    @Test
    void GIVEN_metricsLevel_detailed_THEN_parses_correctly() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(METRICS_LEVEL_CONFIG_NAME, DETAILED_METRICS_LEVEL);
        NucleusEmitterConfiguration config = fromPojo(pojo, logger);
        assertEquals(DETAILED_METRICS_LEVEL, config.getMetricsLevel());
    }

    @Test
    void GIVEN_invalid_metricsLevel_THEN_fails() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(METRICS_LEVEL_CONFIG_NAME, "invalid");
        NucleusEmitterConfiguration config = fromPojo(pojo, logger);
        assertNull(config);
        verify(logger).error(METRICS_LEVEL_CONFIG_PARSE_ERROR_LOG, "invalid");
    }

    @Test
    void GIVEN_outputMode_ipc_THEN_parses_correctly() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(OUTPUT_MODE_CONFIG_NAME, OUTPUT_MODE_IPC);
        NucleusEmitterConfiguration config = fromPojo(pojo, logger);
        assertEquals(OUTPUT_MODE_IPC, config.getOutputMode());
    }

    @Test
    void GIVEN_outputMode_emf_THEN_parses_correctly() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(OUTPUT_MODE_CONFIG_NAME, OUTPUT_MODE_EMF);
        NucleusEmitterConfiguration config = fromPojo(pojo, logger);
        assertEquals(OUTPUT_MODE_EMF, config.getOutputMode());
    }

    @Test
    void GIVEN_outputMode_both_THEN_parses_correctly() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(OUTPUT_MODE_CONFIG_NAME, OUTPUT_MODE_BOTH);
        NucleusEmitterConfiguration config = fromPojo(pojo, logger);
        assertEquals(OUTPUT_MODE_BOTH, config.getOutputMode());
    }

    @Test
    void GIVEN_invalid_outputMode_THEN_fails() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(OUTPUT_MODE_CONFIG_NAME, "invalid");
        NucleusEmitterConfiguration config = fromPojo(pojo, logger);
        assertNull(config);
        verify(logger).error(OUTPUT_MODE_CONFIG_PARSE_ERROR_LOG, "invalid");
    }

    @Test
    void GIVEN_valid_outputDirectory_THEN_parses_correctly() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(OUTPUT_DIRECTORY_CONFIG_NAME, "/custom/path/");
        NucleusEmitterConfiguration config = fromPojo(pojo, logger);
        assertEquals("/custom/path/", config.getOutputDirectory());
    }

    @Test
    void GIVEN_blank_outputDirectory_THEN_uses_default() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(OUTPUT_DIRECTORY_CONFIG_NAME, "");
        NucleusEmitterConfiguration config = fromPojo(pojo, logger);
        assertNotNull(config);
        assertEquals(DEFAULT_OUTPUT_DIRECTORY, config.getOutputDirectory());
    }

    @Test
    void GIVEN_empty_metricsLevel_THEN_uses_default() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(METRICS_LEVEL_CONFIG_NAME, "");
        NucleusEmitterConfiguration config = fromPojo(pojo, logger);
        assertNotNull(config);
        assertEquals(DEFAULT_METRICS_LEVEL, config.getMetricsLevel());
    }

    @Test
    void GIVEN_empty_outputMode_THEN_uses_default() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(OUTPUT_MODE_CONFIG_NAME, "");
        NucleusEmitterConfiguration config = fromPojo(pojo, logger);
        assertNotNull(config);
        assertEquals(DEFAULT_OUTPUT_MODE, config.getOutputMode());
    }

    @Test
    void GIVEN_excludeMounts_as_list_THEN_parses_correctly() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(EXCLUDE_MOUNTS_CONFIG_NAME, Arrays.asList("/mnt/a", "/mnt/b"));
        NucleusEmitterConfiguration config = fromPojo(pojo, logger);
        assertEquals(Arrays.asList("/mnt/a", "/mnt/b"), config.getExcludeMounts());
    }

    @Test
    void GIVEN_excludeMounts_as_string_THEN_parses_correctly() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(EXCLUDE_MOUNTS_CONFIG_NAME, "/mnt/single");
        NucleusEmitterConfiguration config = fromPojo(pojo, logger);
        assertEquals(Arrays.asList("/mnt/single"), config.getExcludeMounts());
    }

    @Test
    void GIVEN_excludeInterfaces_as_list_THEN_parses_correctly() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(EXCLUDE_INTERFACES_CONFIG_NAME, Arrays.asList("eth0", "lo"));
        NucleusEmitterConfiguration config = fromPojo(pojo, logger);
        assertEquals(Arrays.asList("eth0", "lo"), config.getExcludeInterfaces());
    }

    @Test
    void GIVEN_excludeInterfaces_as_string_THEN_parses_correctly() {
        Map<String, Object> pojo = new TreeMap<>();
        pojo.put(EXCLUDE_INTERFACES_CONFIG_NAME, "eth0");
        NucleusEmitterConfiguration config = fromPojo(pojo, logger);
        assertEquals(Arrays.asList("eth0"), config.getExcludeInterfaces());
    }

    @Test
    void GIVEN_basic_metricsLevel_THEN_isDetailedMetrics_returns_false() {
        NucleusEmitterConfiguration config = NucleusEmitterConfiguration.builder()
                .metricsLevel(DEFAULT_METRICS_LEVEL).build();
        assertFalse(config.isDetailedMetrics());
    }

    @Test
    void GIVEN_detailed_metricsLevel_THEN_isDetailedMetrics_returns_true() {
        NucleusEmitterConfiguration config = NucleusEmitterConfiguration.builder()
                .metricsLevel(DETAILED_METRICS_LEVEL).build();
        assertTrue(config.isDetailedMetrics());
    }

    @Test
    void GIVEN_ipc_outputMode_THEN_isEmfEnabled_returns_false() {
        NucleusEmitterConfiguration config = NucleusEmitterConfiguration.builder().outputMode(OUTPUT_MODE_IPC).build();
        assertFalse(config.isEmfEnabled());
    }

    @Test
    void GIVEN_emf_outputMode_THEN_isEmfEnabled_returns_true() {
        NucleusEmitterConfiguration config = NucleusEmitterConfiguration.builder().outputMode(OUTPUT_MODE_EMF).build();
        assertTrue(config.isEmfEnabled());
    }

    @Test
    void GIVEN_both_outputMode_THEN_isEmfEnabled_returns_true() {
        NucleusEmitterConfiguration config = NucleusEmitterConfiguration.builder().outputMode(OUTPUT_MODE_BOTH).build();
        assertTrue(config.isEmfEnabled());
    }
}
