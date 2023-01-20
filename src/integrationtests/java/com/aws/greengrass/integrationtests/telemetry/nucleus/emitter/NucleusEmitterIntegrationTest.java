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
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.mqttclient.PublishRequest;
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
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.event.Level;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.CONFIG_UPDATE_ERROR_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_TELEMETRY_PUBSUB_TOPIC;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.INVALID_PUBLISH_THRESHOLD_LOG;
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
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.DEFAULT_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.INVALID_MQTT_TOPIC_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.INVALID_PUBSUB_PUBLISH_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.INVALID_TELEMETRY_PUBLISH_INTERVALMS_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.INVALID_THRESHOLD_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.NO_CONFIG_OPTIONS_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.REGEX_DEFAULT_TELEMETRY_PUBSUB_TOPIC;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.TEST_ALERTS_MQTT_TOPIC;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.TEST_MQTT_TOPIC;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.format;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.startKernelWithConfig;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(GGExtension.class)
class NucleusEmitterIntegrationTest extends BaseITCase {
    private static Kernel kernel;
    private static final Logger logger = LogManager.getLogger(NucleusEmitterIntegrationTest.class);

    @TempDir
    static Path rootDir;

    @BeforeEach
     void setup(ExtensionContext extensionContext) {
        LogConfig.getRootLogConfig().setLevel(Level.TRACE);
        System.setProperty("root", rootDir.toAbsolutePath().toString());
        kernel = new Kernel();
        NoOpPathOwnershipHandler.register(kernel);
        ignoreExceptionOfType(extensionContext, NumberFormatException.class);
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

    @Test
    void GIVEN_alarms_configured_to_always_trigger_THEN_alarms_triggered() throws Exception {
        MqttClient mockMqttClient = mock(MqttClient.class);
        when(mockMqttClient.connected()).thenReturn(true);
        kernel.getContext().put(MqttClient.class, mockMqttClient);

        startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource("config_alarming.yaml")).toString(), kernel, rootDir);

        // 3 publishes, one for each type of alarm
        CountDownLatch onMqttPublish = new CountDownLatch(3);
        doAnswer(invocation -> {
            // TODO do better
            PublishRequest req = invocation.getArgument(0);
            if (!Objects.equals(TEST_ALERTS_MQTT_TOPIC, req.getTopic())) {
                return CompletableFuture.completedFuture(null);
            }

            // TODO make better assertions on content
            assertNotNull(req.getPayload());

            onMqttPublish.countDown();
            return CompletableFuture.completedFuture(null);
        }).when(mockMqttClient).publish(any());

        assertTrue(onMqttPublish.await(5, TimeUnit.SECONDS));
    }

    private static boolean isJSONValid(String json) {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            List<Metric> retrievedMetrics = mapper.readValue(json, new TypeReference<List<Metric>>(){});
            assertEquals(14, retrievedMetrics.size());
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

    private Topic getConfigTopic(String configOption) {
        return Objects.requireNonNull(kernel.findServiceTopic(AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER)).findTopics(CONFIGURATION_CONFIG_KEY).find(configOption);
    }

}
