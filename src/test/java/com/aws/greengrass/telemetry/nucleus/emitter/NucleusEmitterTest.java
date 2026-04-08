/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter;

import com.aws.greengrass.config.Topics;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.telemetry.impl.Metric;
import com.aws.greengrass.telemetry.nucleus.emitter.metrics.KernelMetricsEmitter;
import com.aws.greengrass.telemetry.nucleus.emitter.metrics.SystemMetricsEmitter;
import com.aws.greengrass.telemetry.nucleus.emitter.publisher.MqttPublisher;
import com.aws.greengrass.telemetry.nucleus.emitter.publisher.PubSubPublisher;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import com.aws.greengrass.testcommons.testutilities.GGServiceTestUtil;
import com.aws.greengrass.testcommons.testutilities.NoOpPathOwnershipHandler;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_TELEMETRY_PUBSUB_TOPIC;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MIN_TELEMETRY_PUBLISH_INTERVAL_MS;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MQTT_TOPIC_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.PUBSUB_PUBLISH_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.DEFAULT_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.INVALID_THRESHOLD_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.MQTT_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.SAMPLE_RAW_KERNEL_METRICS_JSON;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.SAMPLE_RAW_SYSTEM_METRICS_JSON;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.TEST_MQTT_TOPIC;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.readJsonFromFile;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.startKernelWithConfig;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class NucleusEmitterTest extends GGServiceTestUtil {

    @TempDir
    static Path rootDir;

    private Kernel kernel;
    private NucleusEmitter nucleusEmitter;
    private static final ObjectMapper STRICT_MAPPER_JSON = new ObjectMapper(new JsonFactory());

    @Mock
    private SystemMetricsEmitter mockSme;
    @Mock
    private KernelMetricsEmitter mockKme;
    @Mock
    private PubSubPublisher mockPubSubPublisher;
    @Mock
    private MqttPublisher mockMqttPublisher;
    @Mock
    ObjectMapper mockJsonMapper;
    @Mock
    ScheduledExecutorService mockScheduledExecutorService;
    @Mock
    private ScheduledFuture<?> mockScheduledFuture;

    private NucleusEmitter emitter;


    private final List<Metric> mockSmeMetrics = STRICT_MAPPER_JSON.readValue(readJsonFromFile(SAMPLE_RAW_SYSTEM_METRICS_JSON),
            new TypeReference<List<Metric>>(){});
    private final List<Metric> mockKmeMetrics = STRICT_MAPPER_JSON.readValue(readJsonFromFile(SAMPLE_RAW_KERNEL_METRICS_JSON),
            new TypeReference<List<Metric>>(){});
    private final List<Metric> combinedMockMetrics = Stream.of(mockSmeMetrics, mockKmeMetrics)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

    NucleusEmitterTest() throws IOException, URISyntaxException {
        super();
    }

    @BeforeAll
    static void beforeAll()  {
        System.setProperty("root", rootDir.toAbsolutePath().toString());
        STRICT_MAPPER_JSON.findAndRegisterModules();
        STRICT_MAPPER_JSON.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
    }

    @AfterEach
    void teardown() {
        kernel.shutdown();
    }

    @BeforeEach
    void setup() {
        kernel = new Kernel();
        NoOpPathOwnershipHandler.register(kernel);
    }

    @Test
    void GIVEN_valid_metrics_WHEN_publishing_to_iot_core_THEN_ipc_publishes_message() throws Exception {
        initializeMockedConfig();
        when(mockSme.getMetrics()).thenReturn(mockSmeMetrics);
        when(mockKme.getMetrics()).thenReturn(mockKmeMetrics);

        nucleusEmitter = new NucleusEmitter(
                this.config, mockKme, mockPubSubPublisher,
                mockMqttPublisher, mockScheduledExecutorService);
        setSmeField(nucleusEmitter, mockSme);
        nucleusEmitter.retrieveMetricsJson(mockJsonMapper);
        verify(mockSme, times(1)).getMetrics();
        verify(mockKme, times(1)).getMetrics();
        verify(mockJsonMapper, times(1)).writeValueAsString(combinedMockMetrics);
    }

    @Test
    void GIVEN_invalid_metrics_WHEN_publishing_to_iot_core_THEN_error_is_caught(ExtensionContext context) throws Exception {
        initializeMockedConfig();
        when(mockSme.getMetrics()).thenReturn(mockSmeMetrics);
        when(mockKme.getMetrics()).thenReturn(mockKmeMetrics);

        nucleusEmitter = new NucleusEmitter(
                this.config, mockKme, mockPubSubPublisher,
                mockMqttPublisher, mockScheduledExecutorService);
        setSmeField(nucleusEmitter, mockSme);

        doThrow(JsonProcessingException.class).when(mockJsonMapper).writeValueAsString(any());
        ignoreExceptionOfType(context, JsonProcessingException.class);

        nucleusEmitter.retrieveMetricsJson(mockJsonMapper);

        verify(mockSme, times(1)).getMetrics();
        verify(mockKme, times(1)).getMetrics();
        verify(mockJsonMapper, times(1)).writeValueAsString(combinedMockMetrics);
    }

    @Test
    void GIVEN_default_config_WHEN_component_started_THEN_works() throws InterruptedException {

        startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(DEFAULT_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
        Topics configTopic = Objects.requireNonNull(kernel.findServiceTopic(AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER)).findTopics(CONFIGURATION_CONFIG_KEY);
        assertEquals("true", configTopic.find(PUBSUB_PUBLISH_CONFIG_NAME).getOnce());
        assertEquals("", configTopic.find(MQTT_TOPIC_CONFIG_NAME).getOnce());
        assertEquals(Long.toString(DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS), configTopic.find(TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME).getOnce());
    }

    @Test
    void GIVEN_default_config_WHEN_publishInterval_changed_THEN_works() throws InterruptedException {

        startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(DEFAULT_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
        Topics configTopic = Objects.requireNonNull(kernel.findServiceTopic(AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER)).findTopics(CONFIGURATION_CONFIG_KEY);
        configTopic.find(TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME).withValue("10000");
        assertEquals("true", configTopic.find(PUBSUB_PUBLISH_CONFIG_NAME).getOnce());
        assertEquals("", configTopic.find(MQTT_TOPIC_CONFIG_NAME).getOnce());
        assertEquals("10000", configTopic.find(TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME).getOnce());
    }

    @Test
    void GIVEN_mqttPublishing_WHEN_component_started_THEN_it_works() throws InterruptedException {

        startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(MQTT_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
        Topics configTopic = Objects.requireNonNull(kernel.findServiceTopic(AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER)).findTopics(CONFIGURATION_CONFIG_KEY);
        assertEquals("false", configTopic.find(PUBSUB_PUBLISH_CONFIG_NAME).getOnce());
        assertEquals(TEST_MQTT_TOPIC, configTopic.find(MQTT_TOPIC_CONFIG_NAME).getOnce());
        assertEquals(Long.toString(DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS), configTopic.find(TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME).getOnce());
        //Turn off to ensure it shuts down correctly
        configTopic.find(MQTT_TOPIC_CONFIG_NAME).withValue("");

        kernel.getContext().waitForPublishQueueToClear(); //Need to wait for the update to take effect, otherwise we see transient failures
        NucleusEmitterConfiguration currentConfiguration = kernel.getContext().get(NucleusEmitter.class).getCurrentConfiguration().get();
        assertEquals("", currentConfiguration.getMqttTopic());
    }

    @Test
    void GIVEN_mqttPublishing_WHEN_pubSubPublish_enabled_THEN_it_works() throws InterruptedException {
        startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(MQTT_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
        Topics configTopic = Objects.requireNonNull(kernel.findServiceTopic(AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER)).findTopics(CONFIGURATION_CONFIG_KEY);
        configTopic.find(PUBSUB_PUBLISH_CONFIG_NAME).withValue("true");
        assertEquals("true", configTopic.find(PUBSUB_PUBLISH_CONFIG_NAME).getOnce());
        assertEquals(TEST_MQTT_TOPIC, configTopic.find(MQTT_TOPIC_CONFIG_NAME).getOnce());
        assertEquals(Long.toString(DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS), configTopic.find(TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME).getOnce());
    }

    @Test
    void GIVEN_invalid_publish_threshold_WHEN_component_started_THEN_it_reverts_to_minimum() throws InterruptedException {
        startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(INVALID_THRESHOLD_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
        Topics configTopic = Objects.requireNonNull(kernel.findServiceTopic(AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER)).findTopics(CONFIGURATION_CONFIG_KEY);
        assertEquals("true", configTopic.find(PUBSUB_PUBLISH_CONFIG_NAME).getOnce());
        assertEquals(TEST_MQTT_TOPIC, configTopic.find(MQTT_TOPIC_CONFIG_NAME).getOnce());

        kernel.getContext().waitForPublishQueueToClear(); //Need to wait for the update to take effect, otherwise we see transient failures
        //Kernel config is unchanged, plugin configuration is set to min
        assertEquals("100", configTopic.find(TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME).getOnce());
        NucleusEmitterConfiguration currentConfiguration = kernel.getContext().get(NucleusEmitter.class).getCurrentConfiguration().get();
        assertEquals(MIN_TELEMETRY_PUBLISH_INTERVAL_MS, currentConfiguration.getTelemetryPublishIntervalMs());
    }

    @Test
    void GIVEN_invalid_config_option_WHEN_component_started_THEN_it_does_not_update() throws InterruptedException {
        startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(DEFAULT_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
        Topics configTopic = Objects.requireNonNull(kernel.findServiceTopic(AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER)).findTopics(CONFIGURATION_CONFIG_KEY);
        assertEquals("true", configTopic.find(PUBSUB_PUBLISH_CONFIG_NAME).getOnce());
        assertEquals("", configTopic.find(MQTT_TOPIC_CONFIG_NAME).getOnce());
        assertEquals(Long.toString(DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS), configTopic.find(TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME).getOnce());

        //Try to update with invalid value
        configTopic.find(MQTT_TOPIC_CONFIG_NAME).withValue(4545);

        kernel.getContext().waitForPublishQueueToClear(); //Need to wait for the update to take effect, otherwise we see transient failures
        NucleusEmitterConfiguration currentConfiguration = kernel.getContext().get(NucleusEmitter.class).getCurrentConfiguration().get();
        assertEquals("", currentConfiguration.getMqttTopic());
    }

    // --- Unit tests (no kernel) ---

    @Test
    void GIVEN_null_fromPojo_WHEN_handleConfiguration_THEN_returns_early()
            throws Exception {
        initializeMockedConfig();
        emitter = new NucleusEmitter(
                this.config, mockKme, mockPubSubPublisher,
                mockMqttPublisher, mockScheduledExecutorService);
        setSmeField(emitter, mockSme);

        Topics configTopics = mock(Topics.class);
        when(configTopics.toPOJO()).thenReturn(Collections.emptyMap());
        invokeHandleConfiguration(emitter, configTopics);

        assertTrue(emitter.getCurrentConfiguration().get()
                .isPubsubPublish());
    }

    @Test
    void GIVEN_no_changes_WHEN_handleConfiguration_THEN_early_return()
            throws Exception {
        initializeMockedConfig();
        emitter = new NucleusEmitter(
                this.config, mockKme, mockPubSubPublisher,
                mockMqttPublisher, mockScheduledExecutorService);
        setSmeField(emitter, mockSme);

        Map<String, Object> pojo = new HashMap<>();
        pojo.put("pubSubPublish", true);
        pojo.put("mqttTopic", "");
        pojo.put("telemetryPublishIntervalMs", 60000L);

        Topics configTopics = mock(Topics.class);
        when(configTopics.toPOJO()).thenReturn(pojo);
        invokeHandleConfiguration(emitter, configTopics);

        verify(mockScheduledExecutorService, never())
                .scheduleAtFixedRate(
                        any(), anyLong(), anyLong(), any());
    }

    @Test
    void GIVEN_interval_below_minimum_WHEN_handleConfiguration_THEN_clamps()
            throws Exception {
        initializeMockedConfig();
        emitter = new NucleusEmitter(
                this.config, mockKme, mockPubSubPublisher,
                mockMqttPublisher, mockScheduledExecutorService);
        setSmeField(emitter, mockSme);

        Map<String, Object> pojo = new HashMap<>();
        pojo.put("pubSubPublish", true);
        pojo.put("mqttTopic", "");
        pojo.put("telemetryPublishIntervalMs", 100L);

        Topics configTopics = mock(Topics.class);
        when(configTopics.toPOJO()).thenReturn(pojo);
        stubSchedule();
        invokeHandleConfiguration(emitter, configTopics);

        assertEquals(MIN_TELEMETRY_PUBLISH_INTERVAL_MS,
                emitter.getCurrentConfiguration().get()
                        .getTelemetryPublishIntervalMs());
    }

    @Test
    void GIVEN_metricsLevel_changed_WHEN_handleConfiguration_THEN_recreates_sme()
            throws Exception {
        initializeMockedConfig();
        emitter = new NucleusEmitter(
                this.config, mockKme, mockPubSubPublisher,
                mockMqttPublisher, mockScheduledExecutorService);
        setSmeField(emitter, mockSme);
        SystemMetricsEmitter oldSme = getSmeField(emitter);

        Map<String, Object> pojo = new HashMap<>();
        pojo.put("metricsLevel", "detailed");

        Topics configTopics = mock(Topics.class);
        when(configTopics.toPOJO()).thenReturn(pojo);
        stubSchedule();
        invokeHandleConfiguration(emitter, configTopics);

        assertNotNull(getSmeField(emitter));
        assertNotSame(oldSme, getSmeField(emitter));
        assertEquals("detailed",
                emitter.getCurrentConfiguration().get()
                        .getMetricsLevel());
    }

    @Test
    void GIVEN_excludeMounts_changed_WHEN_handleConfiguration_THEN_recreates_sme()
            throws Exception {
        initializeMockedConfig();
        emitter = new NucleusEmitter(
                this.config, mockKme, mockPubSubPublisher,
                mockMqttPublisher, mockScheduledExecutorService);
        setSmeField(emitter, mockSme);

        Map<String, Object> pojo = new HashMap<>();
        pojo.put("excludeMounts", Arrays.asList("/mnt/a"));

        Topics configTopics = mock(Topics.class);
        when(configTopics.toPOJO()).thenReturn(pojo);
        stubSchedule();
        invokeHandleConfiguration(emitter, configTopics);

        assertEquals(Arrays.asList("/mnt/a"),
                emitter.getCurrentConfiguration().get()
                        .getExcludeMounts());
    }

    @Test
    void GIVEN_excludeInterfaces_changed_WHEN_handleConfiguration_THEN_recreates_sme()
            throws Exception {
        initializeMockedConfig();
        emitter = new NucleusEmitter(
                this.config, mockKme, mockPubSubPublisher,
                mockMqttPublisher, mockScheduledExecutorService);
        setSmeField(emitter, mockSme);

        Map<String, Object> pojo = new HashMap<>();
        pojo.put("excludeInterfaces", Arrays.asList("lo"));

        Topics configTopics = mock(Topics.class);
        when(configTopics.toPOJO()).thenReturn(pojo);
        stubSchedule();
        invokeHandleConfiguration(emitter, configTopics);

        assertEquals(Arrays.asList("lo"),
                emitter.getCurrentConfiguration().get()
                        .getExcludeInterfaces());
    }

    @Test
    void GIVEN_normal_config_update_WHEN_handleConfiguration_THEN_schedules_publish()
            throws Exception {
        initializeMockedConfig();
        emitter = new NucleusEmitter(
                this.config, mockKme, mockPubSubPublisher,
                mockMqttPublisher, mockScheduledExecutorService);
        setSmeField(emitter, mockSme);

        Map<String, Object> pojo = new HashMap<>();
        pojo.put("pubSubPublish", false);
        pojo.put("mqttTopic", "test/topic");
        pojo.put("telemetryPublishIntervalMs", 10000L);

        Topics configTopics = mock(Topics.class);
        when(configTopics.toPOJO()).thenReturn(pojo);
        stubSchedule();
        invokeHandleConfiguration(emitter, configTopics);

        NucleusEmitterConfiguration cfg =
                emitter.getCurrentConfiguration().get();
        assertFalse(cfg.isPubsubPublish());
        assertEquals("test/topic", cfg.getMqttTopic());
        assertEquals(10000L, cfg.getTelemetryPublishIntervalMs());
        verify(mockScheduledExecutorService)
                .scheduleAtFixedRate(any(Runnable.class),
                        eq(0L), eq(10000L),
                        eq(TimeUnit.MILLISECONDS));
    }

    @Test
    void GIVEN_existing_future_WHEN_handleConfiguration_THEN_cancels_old()
            throws Exception {
        initializeMockedConfig();
        emitter = new NucleusEmitter(
                this.config, mockKme, mockPubSubPublisher,
                mockMqttPublisher, mockScheduledExecutorService);
        setSmeField(emitter, mockSme);
        setFutureField(emitter, mockScheduledFuture);

        Map<String, Object> pojo = new HashMap<>();
        pojo.put("pubSubPublish", false);

        Topics configTopics = mock(Topics.class);
        when(configTopics.toPOJO()).thenReturn(pojo);
        stubSchedule();
        invokeHandleConfiguration(emitter, configTopics);

        verify(mockScheduledFuture).cancel(false);
    }

    @Test
    void GIVEN_pubsub_and_mqtt_WHEN_publishTelemetry_THEN_publishes_both()
            throws Exception {
        initializeMockedConfig();
        emitter = new NucleusEmitter(
                this.config, mockKme, mockPubSubPublisher,
                mockMqttPublisher, mockScheduledExecutorService);
        setSmeField(emitter, mockSme);
        when(mockSme.getMetrics())
                .thenReturn(Collections.emptyList());
        when(mockKme.getMetrics())
                .thenReturn(Collections.emptyList());

        invokePublishTelemetry(emitter, true,
                DEFAULT_TELEMETRY_PUBSUB_TOPIC, true, "test/topic");

        verify(mockPubSubPublisher).publishMessage(
                any(), eq(DEFAULT_TELEMETRY_PUBSUB_TOPIC));
        verify(mockMqttPublisher).publishMessage(
                any(), eq("test/topic"));
    }

    @Test
    void GIVEN_both_disabled_WHEN_publishTelemetry_THEN_publishes_neither()
            throws Exception {
        initializeMockedConfig();
        emitter = new NucleusEmitter(
                this.config, mockKme, mockPubSubPublisher,
                mockMqttPublisher, mockScheduledExecutorService);
        setSmeField(emitter, mockSme);
        when(mockSme.getMetrics())
                .thenReturn(Collections.emptyList());
        when(mockKme.getMetrics())
                .thenReturn(Collections.emptyList());

        invokePublishTelemetry(emitter, false,
                DEFAULT_TELEMETRY_PUBSUB_TOPIC, false, "");

        verify(mockPubSubPublisher, never())
                .publishMessage(anyString(), anyString());
        verify(mockMqttPublisher, never())
                .publishMessage(anyString(), anyString());
    }

    @Test
    void GIVEN_only_pubsub_WHEN_publishTelemetry_THEN_publishes_pubsub_only()
            throws Exception {
        initializeMockedConfig();
        emitter = new NucleusEmitter(
                this.config, mockKme, mockPubSubPublisher,
                mockMqttPublisher, mockScheduledExecutorService);
        setSmeField(emitter, mockSme);
        when(mockSme.getMetrics())
                .thenReturn(Collections.emptyList());
        when(mockKme.getMetrics())
                .thenReturn(Collections.emptyList());

        invokePublishTelemetry(emitter, true,
                DEFAULT_TELEMETRY_PUBSUB_TOPIC, false, "");

        verify(mockPubSubPublisher).publishMessage(
                any(), eq(DEFAULT_TELEMETRY_PUBSUB_TOPIC));
        verify(mockMqttPublisher, never())
                .publishMessage(anyString(), anyString());
    }

    @Test
    void GIVEN_normal_metrics_WHEN_retrieveMetricsJson_THEN_returns_json()
            throws Exception {
        initializeMockedConfig();
        emitter = new NucleusEmitter(
                this.config, mockKme, mockPubSubPublisher,
                mockMqttPublisher, mockScheduledExecutorService);
        setSmeField(emitter, mockSme);

        List<Metric> metrics =
                Collections.singletonList(new Metric());
        when(mockSme.getMetrics()).thenReturn(metrics);
        when(mockKme.getMetrics()).thenReturn(metrics);

        assertNotNull(
                emitter.retrieveMetricsJson(new ObjectMapper()));
    }

    @Test
    void GIVEN_json_error_WHEN_retrieveMetricsJson_THEN_returns_null(
            ExtensionContext context) throws Exception {
        initializeMockedConfig();
        emitter = new NucleusEmitter(
                this.config, mockKme, mockPubSubPublisher,
                mockMqttPublisher, mockScheduledExecutorService);
        setSmeField(emitter, mockSme);
        when(mockSme.getMetrics())
                .thenReturn(Collections.emptyList());
        when(mockKme.getMetrics())
                .thenReturn(Collections.emptyList());

        ObjectMapper mockMapper = mock(ObjectMapper.class);
        doThrow(JsonProcessingException.class)
                .when(mockMapper).writeValueAsString(any());
        ignoreExceptionOfType(context,
                JsonProcessingException.class);

        assertNull(emitter.retrieveMetricsJson(mockMapper));
    }

    @Test
    void GIVEN_active_future_WHEN_shutdown_THEN_cancels()
            throws Exception {
        initializeMockedConfig();
        emitter = new NucleusEmitter(
                this.config, mockKme, mockPubSubPublisher,
                mockMqttPublisher, mockScheduledExecutorService);
        setSmeField(emitter, mockSme);
        setFutureField(emitter, mockScheduledFuture);
        emitter.shutdown();
        verify(mockScheduledFuture).cancel(true);
    }

    @Test
    void GIVEN_null_future_WHEN_shutdown_THEN_no_error()
            throws Exception {
        initializeMockedConfig();
        emitter = new NucleusEmitter(
                this.config, mockKme, mockPubSubPublisher,
                mockMqttPublisher, mockScheduledExecutorService);
        setSmeField(emitter, mockSme);
        emitter.shutdown();
    }

    // --- helpers ---

    private static void setSmeField(
            NucleusEmitter target, Object value) throws Exception {
        Field f = NucleusEmitter.class.getDeclaredField("sme");
        f.setAccessible(true);
        f.set(target, value);
    }

    private static SystemMetricsEmitter getSmeField(
            NucleusEmitter target) throws Exception {
        Field f = NucleusEmitter.class.getDeclaredField("sme");
        f.setAccessible(true);
        return (SystemMetricsEmitter) f.get(target);
    }

    private static void setFutureField(
            NucleusEmitter target,
            ScheduledFuture<?> future) throws Exception {
        Field f = NucleusEmitter.class
                .getDeclaredField("telemetryPublishFuture");
        f.setAccessible(true);
        f.set(target, future);
    }

    private void stubSchedule() {
        doReturn(mockScheduledFuture)
                .when(mockScheduledExecutorService)
                .scheduleAtFixedRate(any(Runnable.class),
                        anyLong(), anyLong(),
                        any(TimeUnit.class));
    }

    private static void invokeHandleConfiguration(
            NucleusEmitter target,
            Topics configTopics) throws Exception {
        Method m = NucleusEmitter.class.getDeclaredMethod(
                "handleConfiguration", Topics.class);
        m.setAccessible(true);
        m.invoke(target, configTopics);
    }

    private static void invokePublishTelemetry(
            NucleusEmitter target, boolean pubSub,
            String pubSubTopic, boolean mqtt,
            String mqttTopic) throws Exception {
        Method m = NucleusEmitter.class.getDeclaredMethod(
                "publishTelemetry", boolean.class,
                String.class, boolean.class, String.class);
        m.setAccessible(true);
        m.invoke(target, pubSub, pubSubTopic, mqtt, mqttTopic);
    }
}
