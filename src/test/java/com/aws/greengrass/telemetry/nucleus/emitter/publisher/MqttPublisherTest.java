/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter.publisher;

import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.mqttclient.MqttRequestException;
import com.aws.greengrass.mqttclient.PublishRequest;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;

import static com.aws.greengrass.telemetry.nucleus.emitter.TestUtils.SAMPLE_RAW_METRICS_JSON;
import static com.aws.greengrass.telemetry.nucleus.emitter.TestUtils.TEST_MQTT_TOPIC;
import static com.aws.greengrass.telemetry.nucleus.emitter.TestUtils.readJsonFromFile;
import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class MqttPublisherTest {

    @Captor
    private ArgumentCaptor<PublishRequest> publishRequestCaptor;

    @Mock
    private MqttClient mockMqttClient;

    @Test
    void GIVEN_valid_metrics_WHEN_publishing_to_mqtt_THEN_publishes_message() throws IOException, URISyntaxException {

        when(mockMqttClient.publish(any())).thenReturn(CompletableFuture.completedFuture(0));
        when(mockMqttClient.connected()).thenReturn(true);

        MqttPublisher mqttPublisher = new MqttPublisher(mockMqttClient);
        String sampleJson = readJsonFromFile(SAMPLE_RAW_METRICS_JSON);
        mqttPublisher.publishMessage(sampleJson, TEST_MQTT_TOPIC);

        verify(mockMqttClient, times(1)).publish(publishRequestCaptor.capture());
        String payloadString = new String(publishRequestCaptor.getValue().getPayload());
        assertThat(payloadString, Matchers.is(sampleJson));
        assertThat(publishRequestCaptor.getValue().getTopic(), Matchers.is(TEST_MQTT_TOPIC));
    }

    @Test
    void GIVEN_valid_metrics_WHEN_publishing_to_custom_mqtt_topic_THEN_publishes_message() throws IOException, URISyntaxException {

        when(mockMqttClient.publish(any())).thenReturn(CompletableFuture.completedFuture(0));
        when(mockMqttClient.connected()).thenReturn(true);

        MqttPublisher mqttPublisher = new MqttPublisher(mockMqttClient);
        String sampleJson = readJsonFromFile(SAMPLE_RAW_METRICS_JSON);
        mqttPublisher.publishMessage(sampleJson, TEST_MQTT_TOPIC);

        verify(mockMqttClient, times(1)).publish(publishRequestCaptor.capture());
        String payloadString = new String(publishRequestCaptor.getValue().getPayload());
        assertThat(payloadString, Matchers.is(sampleJson));
        assertThat(publishRequestCaptor.getValue().getTopic(), Matchers.is(TEST_MQTT_TOPIC));
    }

    @Test
    void GIVEN_valid_metrics_WHEN_mqtt_publish_throws_error_THEN_error_is_caught(ExtensionContext context) throws IOException, URISyntaxException {
        ignoreExceptionOfType(context, MqttRequestException.class);
        when(mockMqttClient.connected()).thenReturn(true);

        MqttPublisher mqttPublisher = new MqttPublisher(mockMqttClient);
        CompletableFuture<Integer> future = new CompletableFuture<>();
        future.completeExceptionally(new MqttRequestException("Test exception thrown"));
        when(mockMqttClient.publish(any())).thenReturn(future);

        String sampleJson = readJsonFromFile(SAMPLE_RAW_METRICS_JSON);
        mqttPublisher.publishMessage(sampleJson, TEST_MQTT_TOPIC);
        verify(mockMqttClient, times(1)).publish(publishRequestCaptor.capture());
        String payloadString = new String(publishRequestCaptor.getValue().getPayload());
        assertThat(payloadString, Matchers.is(sampleJson));
        assertThat(publishRequestCaptor.getValue().getTopic(), Matchers.is(TEST_MQTT_TOPIC));
    }

    @Test
    void GIVEN_valid_metrics_WHEN_mqtt_offline_THEN_does_not_publish(ExtensionContext context) throws IOException, URISyntaxException {

        MqttPublisher mqttPublisher = new MqttPublisher(mockMqttClient);
        when(mockMqttClient.connected()).thenReturn(false);

        String sampleJson = readJsonFromFile(SAMPLE_RAW_METRICS_JSON);
        mqttPublisher.publishMessage(sampleJson, TEST_MQTT_TOPIC);
        verify(mockMqttClient, times(0)).publish(publishRequestCaptor.capture());
    }


}