/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter;

import com.aws.greengrass.config.Topics;
import com.aws.greengrass.lifecyclemanager.Kernel;
import com.aws.greengrass.telemetry.impl.Metric;
import com.aws.greengrass.telemetry.models.TelemetryAggregation;
import com.aws.greengrass.telemetry.models.TelemetryUnit;
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
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.aws.greengrass.componentmanager.KernelConfigResolver.CONFIGURATION_CONFIG_KEY;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.ALERTS_MQTT_TOPIC_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MIN_TELEMETRY_PUBLISH_INTERVAL_MS;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MQTT_TOPIC_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.PUBSUB_PUBLISH_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.DEFAULT_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.INVALID_THRESHOLD_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.MQTT_NUCLEUS_EMITTER_KERNEL_CONFIG;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.SAMPLE_RAW_KERNEL_METRICS_JSON;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.SAMPLE_RAW_SYSTEM_METRICS_JSON;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.TEST_ALERTS_MQTT_TOPIC;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.TEST_MQTT_TOPIC;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.readJsonFromFile;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.startKernelWithConfig;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
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
    void GIVEN_valid_metrics_WHEN_publishing_to_iot_core_THEN_ipc_publishes_message() throws IOException {
        initializeMockedConfig();
        when(mockSme.getMetrics()).thenReturn(mockSmeMetrics);
        when(mockKme.getMetrics()).thenReturn(mockKmeMetrics);

        nucleusEmitter = new NucleusEmitter(this.config, mockSme, mockKme, mockPubSubPublisher, mockMqttPublisher, mockScheduledExecutorService);
        nucleusEmitter.retrieveMetricsJson(mockJsonMapper);
        verify(mockSme, times(1)).getMetrics();
        verify(mockKme, times(1)).getMetrics();
        verify(mockJsonMapper, times(1)).writeValueAsString(combinedMockMetrics);
    }

    @Test
    void GIVEN_invalid_metrics_WHEN_publishing_to_iot_core_THEN_error_is_caught(ExtensionContext context) throws JsonProcessingException {
        initializeMockedConfig();
        when(mockSme.getMetrics()).thenReturn(mockSmeMetrics);
        when(mockKme.getMetrics()).thenReturn(mockKmeMetrics);

        nucleusEmitter = new NucleusEmitter(this.config, mockSme, mockKme, mockPubSubPublisher, mockMqttPublisher, mockScheduledExecutorService);

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
        assertEquals("", configTopic.find(ALERTS_MQTT_TOPIC_CONFIG_NAME).getOnce());
        assertEquals(Long.toString(DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS), configTopic.find(TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME).getOnce());
    }

    @Test
    void GIVEN_default_config_WHEN_publishInterval_changed_THEN_works() throws InterruptedException {

        startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(DEFAULT_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
        Topics configTopic = Objects.requireNonNull(kernel.findServiceTopic(AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER)).findTopics(CONFIGURATION_CONFIG_KEY);
        configTopic.find(TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME).withValue("10000");
        assertEquals("true", configTopic.find(PUBSUB_PUBLISH_CONFIG_NAME).getOnce());
        assertEquals("", configTopic.find(MQTT_TOPIC_CONFIG_NAME).getOnce());
        assertEquals("", configTopic.find(ALERTS_MQTT_TOPIC_CONFIG_NAME).getOnce());
        assertEquals("10000", configTopic.find(TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME).getOnce());
    }

    @Test
    void GIVEN_mqttPublishing_WHEN_component_started_THEN_it_works() throws InterruptedException {

        startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(MQTT_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
        Topics configTopic = Objects.requireNonNull(kernel.findServiceTopic(AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER)).findTopics(CONFIGURATION_CONFIG_KEY);
        assertEquals("false", configTopic.find(PUBSUB_PUBLISH_CONFIG_NAME).getOnce());
        assertEquals(TEST_MQTT_TOPIC, configTopic.find(MQTT_TOPIC_CONFIG_NAME).getOnce());
        assertEquals(TEST_ALERTS_MQTT_TOPIC, configTopic.find(ALERTS_MQTT_TOPIC_CONFIG_NAME).getOnce());
        assertEquals(Long.toString(DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS), configTopic.find(TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME).getOnce());
        //Turn off to ensure it shuts down correctly
        configTopic.find(MQTT_TOPIC_CONFIG_NAME).withValue("");

        kernel.getContext().waitForPublishQueueToClear(); //Need to wait for the update to take effect, otherwise we see transient failures
        NucleusEmitterConfiguration currentConfiguration = kernel.getContext().get(NucleusEmitter.class).getCurrentConfiguration().get();
        assertEquals("", currentConfiguration.getMqttTopic());
    }

    @Test
    void GIVEN_alarm_config_WHEN_component_started_THEN_alarm_config_is_populated() throws InterruptedException {
        startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource("config_alarms.yaml")).toString(), kernel, rootDir);

        Thread.sleep(1000); // TODO wait for something reliable

        // cpu alarms exist and are populated
        NucleusEmitterConfiguration currentConfiguration = kernel.getContext().get(NucleusEmitter.class).getCurrentConfiguration().get();
        assertNotNull(currentConfiguration.getCpuAlarm());
        assertEquals(">", currentConfiguration.getCpuAlarm().getCondition());
        assertEquals(95, currentConfiguration.getCpuAlarm().getValue());
        assertEquals(1, currentConfiguration.getCpuAlarm().getPeriod());
        assertEquals("MINUTES", currentConfiguration.getCpuAlarm().getPeriodUnit());
        assertEquals(1, currentConfiguration.getCpuAlarm().getDatapoints());
        assertEquals(1, currentConfiguration.getCpuAlarm().getEvaluationPeriod());
    }

    @Test
    void GIVEN_mqttPublishing_WHEN_pubSubPublish_enabled_THEN_it_works() throws InterruptedException {
        startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(MQTT_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
        Topics configTopic = Objects.requireNonNull(kernel.findServiceTopic(AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER)).findTopics(CONFIGURATION_CONFIG_KEY);
        configTopic.find(PUBSUB_PUBLISH_CONFIG_NAME).withValue("true");
        assertEquals("true", configTopic.find(PUBSUB_PUBLISH_CONFIG_NAME).getOnce());
        assertEquals(TEST_MQTT_TOPIC, configTopic.find(MQTT_TOPIC_CONFIG_NAME).getOnce());
        assertEquals(TEST_ALERTS_MQTT_TOPIC, configTopic.find(ALERTS_MQTT_TOPIC_CONFIG_NAME).getOnce());
        assertEquals(Long.toString(DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS), configTopic.find(TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME).getOnce());
    }

    @Test
    void GIVEN_invalid_publish_threshold_WHEN_component_started_THEN_it_reverts_to_minimum() throws InterruptedException {
        startKernelWithConfig(Objects.requireNonNull(NucleusEmitterTestUtils.class.getResource(INVALID_THRESHOLD_NUCLEUS_EMITTER_KERNEL_CONFIG)).toString(), kernel, rootDir);
        Topics configTopic = Objects.requireNonNull(kernel.findServiceTopic(AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER)).findTopics(CONFIGURATION_CONFIG_KEY);
        assertEquals("true", configTopic.find(PUBSUB_PUBLISH_CONFIG_NAME).getOnce());
        assertEquals(TEST_MQTT_TOPIC, configTopic.find(MQTT_TOPIC_CONFIG_NAME).getOnce());
        assertEquals(TEST_ALERTS_MQTT_TOPIC, configTopic.find(ALERTS_MQTT_TOPIC_CONFIG_NAME).getOnce());

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
        assertEquals("", configTopic.find(ALERTS_MQTT_TOPIC_CONFIG_NAME).getOnce());
        assertEquals(Long.toString(DEFAULT_TELEMETRY_PUBLISH_INTERVAL_MS), configTopic.find(TELEMETRY_PUBLISH_INTERVAL_CONFIG_NAME).getOnce());

        //Try to update with invalid value
        configTopic.find(MQTT_TOPIC_CONFIG_NAME).withValue(4545);

        kernel.getContext().waitForPublishQueueToClear(); //Need to wait for the update to take effect, otherwise we see transient failures
        NucleusEmitterConfiguration currentConfiguration = kernel.getContext().get(NucleusEmitter.class).getCurrentConfiguration().get();
        assertEquals("", currentConfiguration.getMqttTopic());
    }

    @Test
    void GIVEN_valid_alert_metrics_WHEN_publishing_to_iot_core_THEN_ipc_publishes_message() {
        initializeMockedConfig();
        List<Metric> mockAlertSmeMetrics = new ArrayList<>();
        // TODO test with CpuMetric
        mockAlertSmeMetrics.add(Metric.builder()
                .namespace(SystemMetricsEmitter.NAMESPACE)
                .name("SystemMemUsagePercentage")
                .value(96.0)
                .aggregation(TelemetryAggregation.Maximum)
                .unit(TelemetryUnit.Percent)
                .build());
        mockAlertSmeMetrics.add(Metric.builder()
                .namespace(SystemMetricsEmitter.NAMESPACE)
                .name("SystemDiskUsagePercentage")
                .value(96.0)
                .aggregation(TelemetryAggregation.Maximum)
                .unit(TelemetryUnit.Percent)
                .build());

        when(mockSme.getMetrics()).thenReturn(mockAlertSmeMetrics);
        when(mockKme.getMetrics()).thenReturn(mockKmeMetrics);

        nucleusEmitter = new NucleusEmitter(this.config, mockSme, mockKme, mockPubSubPublisher, mockMqttPublisher, mockScheduledExecutorService);
        nucleusEmitter.publishAlertTelemetry(true, false, TEST_ALERTS_MQTT_TOPIC);

        verify(mockPubSubPublisher, times(2)).publishMessage(any(), any());
    }
}
