/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.integrationtests.telemetry.nucleus.emitter;

import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.builtin.services.pubsub.PublishEvent;
import com.aws.greengrass.config.Topic;
import com.aws.greengrass.integrationtests.BaseITCase;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.logging.impl.config.LogConfig;
import com.aws.greengrass.telemetry.impl.Metric;
import com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils;
import com.aws.greengrass.testcommons.testutilities.TestUtils;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.NoOpPathOwnershipHandler;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.event.Level;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.CONFIG_UPDATE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_TELEMETRY_PUBSUB_TOPIC;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.EXCLUDE_INTERFACES_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.INVALID_PUBLISH_THRESHOLD_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.METRICS_LEVEL_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MIN_TELEMETRY_PUBLISH_INTERVAL_MS;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MQTT_PUBLISH_STARTING;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MQTT_TOPIC_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MQTT_TOPIC_CONFIG_PARSE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.PUBSUB_PUBLISH_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.PUBSUB_PUBLISH_CONFIG_PARSE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.PUBSUB_PUBLISH_STARTING;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.STARTUP_CONFIGURATION_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.TELEMETRY_PUBLISH_INTERVAL_CONFIG_PARSE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.ALL_NEW_FIELDS_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.BASIC_NEW_FIELDS_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.DEFAULT_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.DETAILED_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.DETAILED_WITH_MQTT_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.INVALID_METRICS_LEVEL_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.INVALID_MQTT_TOPIC_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.INVALID_OUTPUT_DIRECTORY_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.INVALID_OUTPUT_MODE_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.INVALID_PUBSUB_PUBLISH_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.INVALID_TELEMETRY_PUBLISH_INTERVALMS_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.INVALID_THRESHOLD_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.NO_CONFIG_OPTIONS_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.OUTPUT_MODE_BOTH_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.REGEX_DEFAULT_TELEMETRY_PUBSUB_TOPIC;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.TEST_MQTT_TOPIC;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.format;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.startKernelWithConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(GGExtension.class)
class NucleusEmitterIntegrationTest extends BaseITCase {
    private static Kernel kernel;
    private static final Logger logger = LogManager.getLogger(NucleusEmitterIntegrationTest.class);

    @TempDir
    static Path rootDir;

    @BeforeAll
    static void startup() {
        LogConfig.getRootLogConfig().setLevel(Level.TRACE);
    }

    @BeforeEach
     void setup() {
        System.setProperty("root", rootDir.toAbsolutePath().toString());
        kernel = new Kernel();
        NoOpPathOwnershipHandler.register(kernel);
    }

    @AfterEach
    void teardown() {
        kernel.shutdown();
    }

    private void defaultInitialization() throws Exception {
        final CountDownLatch firstPubsubLog = new CountDownLatch(1);
        final CountDownLatch firstMqttLog = new CountDownLatch(1);
        final CountDownLatch firstConfigLog = new CountDownLatch(1);

        try (AutoCloseable listener = TestUtils.createCloseableLogListener((m) -> {
            String stdoutStr = m.getMessage();
            if (stdoutStr == null || stdoutStr.length() == 0) {
                return;
            }
            if (stdoutStr.contains(PUBSUB_PUBLISH_STARTING)) {
                firstPubsubLog.countDown();
            }
            if (stdoutStr.contains(format(MQTT_PUBLISH_STARTING))) {
                firstMqttLog.countDown();
            }
            //Default config is pubsubPublish:true, pubsubTopic:$local/greengrass/telemetry, mqtt_topic:, telemetryPublishIntervalMs:60000
            if (stdoutStr.contains(format(STARTUP_CONFIGURATION_LOG, "true", REGEX_DEFAULT_TELEMETRY_PUBSUB_TOPIC, "", Long.toString(DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS)))) {
                firstConfigLog.countDown();
            }
        })) {

            startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(DEFAULT_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
            assertTrue(firstConfigLog.await(30, TimeUnit.SECONDS), "Running with default config.");
            assertTrue(firstPubsubLog.await(30, TimeUnit.SECONDS), "Pub/sub publish log detected.");
            assertFalse(firstMqttLog.await(30, TimeUnit.SECONDS), "MQTT publish log not detected.");

            checkForPubSubMessages(130000);
        }
    }

    @Test
    void GIVEN_default_initialization_THEN_it_works() throws Exception {
        defaultInitialization();
    }

    @Test
    void GIVEN_config_options_changing_THEN_it_works() throws Exception {

        defaultInitialization();

        //--------------------------------------------------
        //Change telemetryPublishInterval=5000ms

        final CountDownLatch firstPubsubLog = new CountDownLatch(1);
        final CountDownLatch firstMqttLog = new CountDownLatch(1);
        final CountDownLatch firstConfigLog = new CountDownLatch(1);
        try (AutoCloseable listener = TestUtils.createCloseableLogListener((m) -> {
            String stdoutStr = m.getMessage();
            if (stdoutStr == null || stdoutStr.length() == 0) {return;}
            //Config should now be pubsub:true, pubsub_topic:$local/greengrass/telemetry, mqtt_topic:, telemetryPublishIntervalMs:5000
            if (stdoutStr.contains(format(STARTUP_CONFIGURATION_LOG, "true", REGEX_DEFAULT_TELEMETRY_PUBSUB_TOPIC, "", "5000"))) {
                firstConfigLog.countDown();
            }
            if (stdoutStr.contains(PUBSUB_PUBLISH_STARTING)) {
                firstPubsubLog.countDown();
            }
            if (stdoutStr.contains(MQTT_PUBLISH_STARTING)) {
                firstMqttLog.countDown();
            }
        })) {
            getConfigTopic(TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME).withValue(5000);

            assertTrue(firstConfigLog.await(30, TimeUnit.SECONDS), "Running with expected config.");
            assertTrue(firstPubsubLog.await(15, TimeUnit.SECONDS), "Pub/sub publish log detected.");
            assertFalse(firstMqttLog.await(15, TimeUnit.SECONDS), "MQTT publish log not detected.");
        }

        //--------------------------------------------------
        //Change mqttTopic=greengrass/nucleus/telemetry

        //Cannot reuse the previous countdownlatch
        final CountDownLatch secondPubsubLog = new CountDownLatch(1);
        final CountDownLatch secondMqttLog = new CountDownLatch(1);
        final CountDownLatch secondConfigLog = new CountDownLatch(1);
        try (AutoCloseable listener = TestUtils.createCloseableLogListener((m) -> {
            String stdoutStr = m.getMessage();
            if (stdoutStr == null || stdoutStr.length() == 0) {return;}
            if (stdoutStr.contains(PUBSUB_PUBLISH_STARTING)) {
                secondPubsubLog.countDown();
            }
            if (stdoutStr.contains(MQTT_PUBLISH_STARTING)) {
                secondMqttLog.countDown();
            }
            //Config should now be pubSubPublish:true, pubSubTopic:$local/greengrass/telemetry, mqttTopic:"greengrass/nucleus/telemetry", telemetryPublishIntervalMs:5000
            if (stdoutStr.contains(format(STARTUP_CONFIGURATION_LOG, "true", REGEX_DEFAULT_TELEMETRY_PUBSUB_TOPIC, "greengrass/nucleus/telemetry", "5000"))) {
                secondConfigLog.countDown();
            }
        })) {

            //Change MQTT Topic
            getConfigTopic(MQTT_TOPIC_CONFIG_NAME).withValue("greengrass/nucleus/telemetry");

            assertTrue(secondConfigLog.await(30, TimeUnit.SECONDS), "Running with expected config.");
            assertTrue(secondPubsubLog.await(15, TimeUnit.SECONDS), "Pub/sub publish log detected.");
            assertTrue(secondMqttLog.await(15, TimeUnit.SECONDS), "MQTT publish log detected.");
            checkForPubSubMessages(20000);
        }

        //--------------------------------------------------
        //Change pubsubPublish=false

        final CountDownLatch thirdPubsubLog = new CountDownLatch(1);
        final CountDownLatch thirdMqttLog = new CountDownLatch(1);
        final CountDownLatch thirdConfigLog = new CountDownLatch(1);
        try (AutoCloseable listener = TestUtils.createCloseableLogListener((m) -> {
            String stdoutStr = m.getMessage();
            if (stdoutStr == null || stdoutStr.length() == 0) {return;}
            //Config should now be pubSubPublish:false, pubSubTopic:$local/greengrass/telemetry, mqttTopic:"greengrass/nucleus/telemetry" telemetryPublishIntervalMs:5000
            if (stdoutStr.contains(format(STARTUP_CONFIGURATION_LOG, "false", REGEX_DEFAULT_TELEMETRY_PUBSUB_TOPIC, "greengrass/nucleus/telemetry", "5000"))) {
                thirdConfigLog.countDown();
            }
            if (stdoutStr.contains(PUBSUB_PUBLISH_STARTING)) {
                thirdPubsubLog.countDown();
            }
            if (stdoutStr.contains(MQTT_PUBLISH_STARTING)) {
                thirdMqttLog.countDown();
            }
        })) {
            //Turn pubsub off
            getConfigTopic(PUBSUB_PUBLISH_CONFIG_NAME).withValue(false);

            assertTrue(thirdConfigLog.await(30, TimeUnit.SECONDS), "Running with expected config.");
            assertFalse(thirdPubsubLog.await(15, TimeUnit.SECONDS), "Pub/sub publish log not detected.");
            assertTrue(thirdMqttLog.await(15, TimeUnit.SECONDS), "MQTT publish log detected.");
        }

        //--------------------------------------------------
        //Change mqttPublish=false

        final CountDownLatch fourthPubsubLog = new CountDownLatch(1);
        final CountDownLatch fourthMqttLog = new CountDownLatch(1);
        final CountDownLatch fourthConfigLog = new CountDownLatch(1);
        try (AutoCloseable listener = TestUtils.createCloseableLogListener((m) -> {
            String stdoutStr = m.getMessage();
            if (stdoutStr == null || stdoutStr.length() == 0) {return;}
            //Config should now be pubSubPublish:false, pubSubTopic:$local/greengrass/telemetry, mqttTopic:"" telemetryPublishIntervalMs:5000
            if (stdoutStr.contains(format(STARTUP_CONFIGURATION_LOG, "false", REGEX_DEFAULT_TELEMETRY_PUBSUB_TOPIC, "", "5000"))) {
                fourthConfigLog.countDown();
            }
            if (stdoutStr.contains(PUBSUB_PUBLISH_STARTING)) {
                fourthPubsubLog.countDown();
            }
            if (stdoutStr.contains(MQTT_PUBLISH_STARTING)) {
                fourthMqttLog.countDown();
            }
        })) {
            //Turn MQTT publishing off
            getConfigTopic(MQTT_TOPIC_CONFIG_NAME).withValue("");

            assertTrue(fourthConfigLog.await(30, TimeUnit.SECONDS), "Running with expected config.");
            assertFalse(fourthPubsubLog.await(15, TimeUnit.SECONDS), "Pub/sub publish log not detected.");
            assertFalse(fourthMqttLog.await(15, TimeUnit.SECONDS), "MQTT publish log not detected.");
        }
    }

    @Test
    void GIVEN_invalid_publish_threshold_THEN_it_reverts_to_minimum() throws Exception {
        final CountDownLatch logFound = new CountDownLatch(1);
        final CountDownLatch configLog = new CountDownLatch(1);
        final CountDownLatch pubsubLog = new CountDownLatch(1);
        final CountDownLatch mqttLog = new CountDownLatch(1);
        try (AutoCloseable listener = TestUtils.createCloseableLogListener((m) -> {
            String stdoutStr = m.getMessage();
            if (stdoutStr == null || stdoutStr.length() == 0) {return;}
            if (stdoutStr.contains(INVALID_PUBLISH_THRESHOLD_LOG)) {
                logFound.countDown();
            }
            //Config should be pubsubPublish:true, pubsubTopic:$local/greengrass/telemetry, mqttTopic:test/topic, telemetryPublishIntervalMs:500
            if (stdoutStr.contains(format(STARTUP_CONFIGURATION_LOG, "true", REGEX_DEFAULT_TELEMETRY_PUBSUB_TOPIC, TEST_MQTT_TOPIC, Long.toString(MIN_TELEMETRY_PUBLISH_INTERVAL_MS)))) {
                configLog.countDown();
            }
            if (stdoutStr.contains(PUBSUB_PUBLISH_STARTING)) {
                pubsubLog.countDown();
            }
            if (stdoutStr.contains(MQTT_PUBLISH_STARTING)) {
                mqttLog.countDown();
            }
        })) {
            startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(INVALID_THRESHOLD_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
            assertTrue(logFound.await(30, TimeUnit.SECONDS), "Invalid threshold detected.");
            assertTrue(configLog.await(30, TimeUnit.SECONDS), "Running with expected config.");
            assertTrue(pubsubLog.await(30, TimeUnit.SECONDS), "Pub/sub publish log detected.");
            assertTrue(mqttLog.await(30, TimeUnit.SECONDS), "MQTT publish log detected.");
            checkForPubSubMessages(20000);
        }
    }

    @Test
    void GIVEN_no_config_options_THEN_it_uses_default() throws Exception {
        final CountDownLatch logFound = new CountDownLatch(1);
        final CountDownLatch configLog = new CountDownLatch(1);
        final CountDownLatch pubsubLog = new CountDownLatch(1);
        final CountDownLatch mqttLog = new CountDownLatch(1);
        try (AutoCloseable listener = TestUtils.createCloseableLogListener((m) -> {
            String stdoutStr = m.getMessage();
            if (stdoutStr == null || stdoutStr.length() == 0) {return;}
            if (stdoutStr.contains(CONFIG_UPDATE_ERROR_LOG)) {
                logFound.countDown();
            }
            //Config should be pubsubPublish:true, pubsubTopic:$local/greengrass/telemetry, mqttTopic:, telemetryPublishIntervalMs:60000
            if (stdoutStr.contains(format(STARTUP_CONFIGURATION_LOG, "true", REGEX_DEFAULT_TELEMETRY_PUBSUB_TOPIC, "", Long.toString(DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS)))) {
                configLog.countDown();
            }
            if (stdoutStr.contains(PUBSUB_PUBLISH_STARTING)) {
                pubsubLog.countDown();
            }
            if (stdoutStr.contains(MQTT_PUBLISH_STARTING)) {
                mqttLog.countDown();
            }
        })) {
            startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(NO_CONFIG_OPTIONS_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
            assertTrue(logFound.await(30, TimeUnit.SECONDS), "Invalid config options detected.");
            assertTrue(configLog.await(30, TimeUnit.SECONDS), "Running with default config.");
            assertTrue(pubsubLog.await(30, TimeUnit.SECONDS), "Pub/sub publish log detected.");
            assertFalse(mqttLog.await(30, TimeUnit.SECONDS), "MQTT publish log detected.");
            checkForPubSubMessages(130000);
        }
    }

    @Test
    void GIVEN_invalid_pubSubPublish_option_THEN_it_uses_default() throws Exception {
        final CountDownLatch logFound = new CountDownLatch(1);
        final CountDownLatch configLog = new CountDownLatch(1);
        final CountDownLatch pubsubLog = new CountDownLatch(1);
        final CountDownLatch mqttLog = new CountDownLatch(1);
        try (AutoCloseable listener = TestUtils.createCloseableLogListener((m) -> {
            String stdoutStr = m.getMessage();
            if (stdoutStr == null || stdoutStr.length() == 0) {return;}
            if (stdoutStr.contains(format(PUBSUB_PUBLISH_CONFIG_PARSE_ERROR_LOG, "garbage"))) {
                logFound.countDown();
            }
            //Config should be pubsubPublish:true, pubsubTopic:$local/greengrass/telemetry, mqttTopic:, telemetryPublishIntervalMs:60000
            if (stdoutStr.contains(format(STARTUP_CONFIGURATION_LOG, "true", REGEX_DEFAULT_TELEMETRY_PUBSUB_TOPIC, "", Long.toString(DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS)))) {
                configLog.countDown();
            }
            if (stdoutStr.contains(PUBSUB_PUBLISH_STARTING)) {
                pubsubLog.countDown();
            }
            if (stdoutStr.contains(MQTT_PUBLISH_STARTING)) {
                mqttLog.countDown();
            }
        })) {
            startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(INVALID_PUBSUB_PUBLISH_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
            assertTrue(logFound.await(30, TimeUnit.SECONDS), "Invalid config options detected.");
            assertTrue(configLog.await(30, TimeUnit.SECONDS), "Running with default config.");
            assertTrue(pubsubLog.await(30, TimeUnit.SECONDS), "Pub/sub publish log detected.");
            assertFalse(mqttLog.await(30, TimeUnit.SECONDS), "MQTT publish log detected.");
            checkForPubSubMessages(130000);
        }
    }

    @Test
    void GIVEN_invalid_telemetryPublishIntervalMs_option_THEN_it_uses_default() throws Exception {
        final CountDownLatch logFound = new CountDownLatch(1);
        final CountDownLatch configLog = new CountDownLatch(1);
        final CountDownLatch pubsubLog = new CountDownLatch(1);
        final CountDownLatch mqttLog = new CountDownLatch(1);
        try (AutoCloseable listener = TestUtils.createCloseableLogListener((m) -> {
            String stdoutStr = m.getMessage();
            if (stdoutStr == null || stdoutStr.length() == 0) {return;}
            if (stdoutStr.contains(format(TELEMETRY_PUBLISH_INTERVAL_CONFIG_PARSE_ERROR_LOG, "garbage"))) {
                logFound.countDown();
            }
            //Config should be pubsubPublish:true, pubsubTopic:$local/greengrass/telemetry, mqttTopic:, telemetryPublishIntervalMs:60000
            if (stdoutStr.contains(format(STARTUP_CONFIGURATION_LOG, "true", REGEX_DEFAULT_TELEMETRY_PUBSUB_TOPIC, "", Long.toString(DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS)))) {
                configLog.countDown();
            }
            if (stdoutStr.contains(PUBSUB_PUBLISH_STARTING)) {
                pubsubLog.countDown();
            }
            if (stdoutStr.contains(MQTT_PUBLISH_STARTING)) {
                mqttLog.countDown();
            }
        })) {
            startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(INVALID_TELEMETRY_PUBLISH_INTERVALMS_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
            assertTrue(logFound.await(30, TimeUnit.SECONDS), "Invalid config options detected.");
            assertTrue(configLog.await(30, TimeUnit.SECONDS), "Running with default config.");
            assertTrue(pubsubLog.await(30, TimeUnit.SECONDS), "Pub/sub publish log detected.");
            assertFalse(mqttLog.await(30, TimeUnit.SECONDS), "MQTT publish log detected.");
            checkForPubSubMessages(130000);
        }
    }

    @Test
    void GIVEN_invalid_mqttTopic_option_THEN_it_uses_default() throws Exception {
        final CountDownLatch logFound = new CountDownLatch(1);
        final CountDownLatch configLog = new CountDownLatch(1);
        final CountDownLatch pubsubLog = new CountDownLatch(1);
        final CountDownLatch mqttLog = new CountDownLatch(1);
        try (AutoCloseable listener = TestUtils.createCloseableLogListener((m) -> {
            String stdoutStr = m.getMessage();
            if (stdoutStr == null || stdoutStr.length() == 0) {return;}
            if (stdoutStr.contains(format(MQTT_TOPIC_CONFIG_PARSE_ERROR_LOG, "4545"))) {
                logFound.countDown();
            }
            //Config should be pubsubPublish:true, pubsubTopic:$local/greengrass/telemetry, mqttTopic:, telemetryPublishIntervalMs:60000
            if (stdoutStr.contains(format(STARTUP_CONFIGURATION_LOG, "true", REGEX_DEFAULT_TELEMETRY_PUBSUB_TOPIC, "", Long.toString(DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS)))) {
                configLog.countDown();
            }
            if (stdoutStr.contains(PUBSUB_PUBLISH_STARTING)) {
                pubsubLog.countDown();
            }
            if (stdoutStr.contains(MQTT_PUBLISH_STARTING)) {
                mqttLog.countDown();
            }
        })) {
            startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(INVALID_MQTT_TOPIC_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
            assertTrue(logFound.await(30, TimeUnit.SECONDS), "Invalid config options detected.");
            assertTrue(configLog.await(30, TimeUnit.SECONDS), "Running with default config.");
            assertTrue(pubsubLog.await(30, TimeUnit.SECONDS), "Pub/sub publish log detected.");
            assertFalse(mqttLog.await(30, TimeUnit.SECONDS), "MQTT publish log detected.");
            checkForPubSubMessages(130000);
        }
    }

    private static boolean isJSONValid(String json) {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            List<Metric> retrievedMetrics = mapper.readValue(json, new TypeReference<List<Metric>>(){});
            assertEquals(12, retrievedMetrics.size());
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private void checkForPubSubMessages(int timeout){
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Consumer<PublishEvent> consumer = getConsumer(countDownLatch);

        try {
            final PubSubIPCEventStreamAgent pubSubIPCEventStreamAgent = kernel.getContext().get(PubSubIPCEventStreamAgent.class);
            pubSubIPCEventStreamAgent.subscribe(DEFAULT_TELEMETRY_PUBSUB_TOPIC, consumer, AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER);
            assertTrue(countDownLatch.await(timeout, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
           logger.error("Failed to receive pub/sub message", e);
        }
    }

    private static Consumer<PublishEvent> getConsumer(CountDownLatch cdl) {
        return subscriptionResponseMessage -> {
            if (isJSONValid(new String(subscriptionResponseMessage.getPayload(), StandardCharsets.UTF_8))){
                cdl.countDown();
            }
        };
    }
    @Test
    void GIVEN_all_new_config_fields_WHEN_component_started_THEN_it_works() throws Exception {
        final CountDownLatch configLog = new CountDownLatch(1);
        final CountDownLatch pubsubLog = new CountDownLatch(1);

        try (AutoCloseable listener = TestUtils.createCloseableLogListener((m) -> {
            String stdoutStr = m.getMessage();
            if (stdoutStr == null || stdoutStr.length() == 0) {return;}
            if (stdoutStr.contains(format(STARTUP_CONFIGURATION_LOG, "true", REGEX_DEFAULT_TELEMETRY_PUBSUB_TOPIC, "", Long.toString(DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS)))) {
                configLog.countDown();
            }
            if (stdoutStr.contains(PUBSUB_PUBLISH_STARTING)) {
                pubsubLog.countDown();
            }
        })) {
            startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(ALL_NEW_FIELDS_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
            assertTrue(configLog.await(30, TimeUnit.SECONDS), "Running with all new fields config.");
            assertTrue(pubsubLog.await(30, TimeUnit.SECONDS), "Pub/sub publish log detected.");
        }
    }

    @Test
    void GIVEN_basic_new_config_fields_WHEN_component_started_THEN_it_works() throws Exception {
        final CountDownLatch configLog = new CountDownLatch(1);
        final CountDownLatch pubsubLog = new CountDownLatch(1);

        try (AutoCloseable listener = TestUtils.createCloseableLogListener((m) -> {
            String stdoutStr = m.getMessage();
            if (stdoutStr == null || stdoutStr.length() == 0) {return;}
            if (stdoutStr.contains(format(STARTUP_CONFIGURATION_LOG, "true", REGEX_DEFAULT_TELEMETRY_PUBSUB_TOPIC, "", Long.toString(DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS)))) {
                configLog.countDown();
            }
            if (stdoutStr.contains(PUBSUB_PUBLISH_STARTING)) {
                pubsubLog.countDown();
            }
        })) {
            startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(BASIC_NEW_FIELDS_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
            assertTrue(configLog.await(30, TimeUnit.SECONDS), "Running with basic new fields config.");
            assertTrue(pubsubLog.await(30, TimeUnit.SECONDS), "Pub/sub publish log detected.");
        }
    }
    @Test
    void GIVEN_invalid_metricsLevel_option_THEN_it_uses_default() throws Exception {
        final CountDownLatch errorLog = new CountDownLatch(1);
        final CountDownLatch pubsubLog = new CountDownLatch(1);
        try (AutoCloseable listener = TestUtils.createCloseableLogListener((m) -> {
            String stdoutStr = m.getMessage();
            if (stdoutStr == null || stdoutStr.length() == 0) {return;}
            if (stdoutStr.contains("Could not parse the metricsLevel")) {
                errorLog.countDown();
            }
            if (stdoutStr.contains(PUBSUB_PUBLISH_STARTING)) {
                pubsubLog.countDown();
            }
        })) {
            startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(INVALID_METRICS_LEVEL_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
            assertTrue(errorLog.await(30, TimeUnit.SECONDS), "metricsLevel parse error log detected.");
            assertTrue(pubsubLog.await(30, TimeUnit.SECONDS), "Pub/sub publish log detected.");
        }
    }

    @Test
    void GIVEN_invalid_outputMode_option_THEN_it_uses_default() throws Exception {
        final CountDownLatch errorLog = new CountDownLatch(1);
        final CountDownLatch pubsubLog = new CountDownLatch(1);
        try (AutoCloseable listener = TestUtils.createCloseableLogListener((m) -> {
            String stdoutStr = m.getMessage();
            if (stdoutStr == null || stdoutStr.length() == 0) {return;}
            if (stdoutStr.contains("Could not parse the outputMode")) {
                errorLog.countDown();
            }
            if (stdoutStr.contains(PUBSUB_PUBLISH_STARTING)) {
                pubsubLog.countDown();
            }
        })) {
            startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(INVALID_OUTPUT_MODE_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
            assertTrue(errorLog.await(30, TimeUnit.SECONDS), "outputMode parse error log detected.");
            assertTrue(pubsubLog.await(30, TimeUnit.SECONDS), "Pub/sub publish log detected.");
        }
    }

    @Test
    void GIVEN_blank_outputDirectory_option_THEN_it_uses_default() throws Exception {
        final CountDownLatch pubsubLog = new CountDownLatch(1);
        try (AutoCloseable listener = TestUtils.createCloseableLogListener((m) -> {
            String stdoutStr = m.getMessage();
            if (stdoutStr == null || stdoutStr.length() == 0) {return;}
            if (stdoutStr.contains(PUBSUB_PUBLISH_STARTING)) {
                pubsubLog.countDown();
            }
        })) {
            startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(INVALID_OUTPUT_DIRECTORY_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
            assertTrue(pubsubLog.await(30, TimeUnit.SECONDS), "Pub/sub publish log detected.");
        }
    }

    @Test
    void GIVEN_detailed_metrics_config_WHEN_component_started_THEN_disk_metrics_collected() throws Exception {
        CountDownLatch pubsubLog = new CountDownLatch(1);
        CountDownLatch diskMetricLatch = new CountDownLatch(1);
        try (AutoCloseable l = TestUtils.createCloseableLogListener(m -> {
            String stdoutStr = m.getMessage();
            if (stdoutStr != null && stdoutStr.contains(PUBSUB_PUBLISH_STARTING)) {
                pubsubLog.countDown();
            }
        })) {
            startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(
                    DETAILED_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
            assertTrue(pubsubLog.await(30, TimeUnit.SECONDS), "Pub/sub publish log detected.");

            Consumer<PublishEvent> consumer = event -> {
                String payload = new String(event.getPayload(), StandardCharsets.UTF_8);
                if (payload.contains("DiskUsagePercent")) {
                    diskMetricLatch.countDown();
                }
            };
            PubSubIPCEventStreamAgent agent = kernel.getContext()
                    .get(PubSubIPCEventStreamAgent.class);
            agent.subscribe(DEFAULT_TELEMETRY_PUBSUB_TOPIC, consumer,
                    AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER);
            assertTrue(diskMetricLatch.await(130000, TimeUnit.MILLISECONDS),
                    "PubSub message contains DiskUsagePercent.");
        }
    }

    @Test
    void GIVEN_detailed_metrics_with_mqtt_WHEN_component_started_THEN_both_publish_paths() throws Exception {
        final CountDownLatch pubsubLog = new CountDownLatch(1);
        final CountDownLatch mqttLog = new CountDownLatch(1);
        final CountDownLatch configLog = new CountDownLatch(1);
        try (AutoCloseable listener = TestUtils.createCloseableLogListener((m) -> {
            String stdoutStr = m.getMessage();
            if (stdoutStr == null || stdoutStr.length() == 0) {return;}
            if (stdoutStr.contains(PUBSUB_PUBLISH_STARTING)) {
                pubsubLog.countDown();
            }
            if (stdoutStr.contains(MQTT_PUBLISH_STARTING)) {
                mqttLog.countDown();
            }
            if (stdoutStr.contains(format(STARTUP_CONFIGURATION_LOG, "true",
                    REGEX_DEFAULT_TELEMETRY_PUBSUB_TOPIC, TEST_MQTT_TOPIC,
                    Long.toString(DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS)))) {
                configLog.countDown();
            }
        })) {
            startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(
                    DETAILED_WITH_MQTT_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
            assertTrue(configLog.await(30, TimeUnit.SECONDS), "Config log detected.");
            assertTrue(pubsubLog.await(30, TimeUnit.SECONDS), "Pub/sub publish log detected.");
            assertTrue(mqttLog.await(30, TimeUnit.SECONDS), "MQTT publish log detected.");
        }
    }

    @Test
    void GIVEN_output_mode_both_WHEN_component_started_THEN_it_works() throws Exception {
        final CountDownLatch configLog = new CountDownLatch(1);
        final CountDownLatch pubsubLog = new CountDownLatch(1);
        try (AutoCloseable listener = TestUtils.createCloseableLogListener((m) -> {
            String stdoutStr = m.getMessage();
            if (stdoutStr == null || stdoutStr.length() == 0) {return;}
            if (stdoutStr.contains(format(STARTUP_CONFIGURATION_LOG, "true",
                    REGEX_DEFAULT_TELEMETRY_PUBSUB_TOPIC, "",
                    Long.toString(DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS)))) {
                configLog.countDown();
            }
            if (stdoutStr.contains(PUBSUB_PUBLISH_STARTING)) {
                pubsubLog.countDown();
            }
        })) {
            startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(
                    OUTPUT_MODE_BOTH_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
            assertTrue(configLog.await(30, TimeUnit.SECONDS), "Config log detected.");
            assertTrue(pubsubLog.await(30, TimeUnit.SECONDS), "Pub/sub publish log detected.");
        }
    }

    @Test
    void GIVEN_default_config_WHEN_metricsLevel_changed_to_detailed_THEN_sme_recreated() throws Exception {
        final CountDownLatch startupLog = new CountDownLatch(1);
        final CountDownLatch configLog = new CountDownLatch(1);
        final CountDownLatch diskMetricLatch = new CountDownLatch(1);
        try (AutoCloseable listener = TestUtils.createCloseableLogListener((m) -> {
            String stdoutStr = m.getMessage();
            if (stdoutStr == null || stdoutStr.length() == 0) {return;}
            if (stdoutStr.contains(PUBSUB_PUBLISH_STARTING)) {
                startupLog.countDown();
            }
            if (stdoutStr.contains(format(STARTUP_CONFIGURATION_LOG,
                    "true", REGEX_DEFAULT_TELEMETRY_PUBSUB_TOPIC,
                    "",
                    Long.toString(
                            DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS)))) {
                configLog.countDown();
            }
        })) {
            startKernelWithConfig(Objects.requireNonNull(
                    NucleusEmitterTestUtils.class.getResource(
                            DEFAULT_NUCLEUS_EMITTER_KERNEL_CONFIG))
                    .toString(), kernel, rootDir);
            assertTrue(startupLog.await(30, TimeUnit.SECONDS),
                    "Component started.");

            Consumer<PublishEvent> consumer = event -> {
                String payload = new String(
                        event.getPayload(), StandardCharsets.UTF_8);
                if (payload.contains("DiskUsagePercent")) {
                    diskMetricLatch.countDown();
                }
            };
            PubSubIPCEventStreamAgent agent = kernel.getContext()
                    .get(PubSubIPCEventStreamAgent.class);
            agent.subscribe(DEFAULT_TELEMETRY_PUBSUB_TOPIC, consumer,
                    AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER);

            getConfigTopic(METRICS_LEVEL_CONFIG_NAME)
                    .withValue("detailed");
            assertTrue(configLog.await(30, TimeUnit.SECONDS),
                    "Config log after metricsLevel change.");
            assertTrue(
                    diskMetricLatch.await(130000, TimeUnit.MILLISECONDS),
                    "PubSub message contains disk metrics.");
        }
    }

    @Test
    void GIVEN_default_config_WHEN_excludeInterfaces_changed_THEN_sme_recreated() throws Exception {
        defaultInitialization();

        final CountDownLatch configLog = new CountDownLatch(1);
        final CountDownLatch pubsubLog = new CountDownLatch(1);
        try (AutoCloseable listener = TestUtils.createCloseableLogListener((m) -> {
            String stdoutStr = m.getMessage();
            if (stdoutStr == null || stdoutStr.length() == 0) {return;}
            if (stdoutStr.contains(format(STARTUP_CONFIGURATION_LOG, "true",
                    REGEX_DEFAULT_TELEMETRY_PUBSUB_TOPIC, "",
                    Long.toString(DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS)))) {
                configLog.countDown();
            }
            if (stdoutStr.contains(PUBSUB_PUBLISH_STARTING)) {
                pubsubLog.countDown();
            }
        })) {
            getConfigTopic(EXCLUDE_INTERFACES_CONFIG_NAME).withValue("docker0");
            assertTrue(configLog.await(30, TimeUnit.SECONDS), "Config log after excludeInterfaces change.");
            assertTrue(pubsubLog.await(30, TimeUnit.SECONDS), "Pub/sub publish log detected.");
        }
    }

    private Topic getConfigTopic(String configOption) {
        return Objects.requireNonNull(kernel.findServiceTopic(AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER)).findTopics(CONFIGURATION_CONFIG_KEY).lookup(configOption);
    }

}
