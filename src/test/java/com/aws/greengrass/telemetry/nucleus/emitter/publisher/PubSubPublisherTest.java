/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter.publisher;

import java.nio.charset.StandardCharsets;

import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;

import java.io.IOException;
import java.net.URISyntaxException;

import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_TELEMETRY_PUBSUB_TOPIC;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.SAMPLE_RAW_METRICS_JSON;
import static com.aws.greengrass.telemetry.nucleus.emitter.NucleusEmitterTestUtils.readJsonFromFile;

import static com.aws.greengrass.testcommons.testutilities.ExceptionLogProtector.ignoreExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class PubSubPublisherTest {
    @Mock
    private PubSubIPCEventStreamAgent mockPubSubIPCEventStreamAgent;

    @Test
    void GIVEN_valid_metrics_WHEN_publishing_THEN_ipc_publishes_message() throws IOException, URISyntaxException {

        PubSubPublisher pubSubPublisher = new PubSubPublisher(mockPubSubIPCEventStreamAgent);
        String sampleJson = readJsonFromFile(SAMPLE_RAW_METRICS_JSON);
        pubSubPublisher.publishMessage(sampleJson, DEFAULT_TELEMETRY_PUBSUB_TOPIC);

        verify(mockPubSubIPCEventStreamAgent, times(1)).publish(DEFAULT_TELEMETRY_PUBSUB_TOPIC,
                sampleJson.getBytes(StandardCharsets.UTF_8), AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER);
    }

    @Test
    void GIVEN_valid_metrics_WHEN_publish_throws_error_THEN_error_is_caught(ExtensionContext context) throws IOException, URISyntaxException {
        ignoreExceptionOfType(context, InvalidArgumentsError.class);
        InvalidArgumentsError e = new InvalidArgumentsError();
        doThrow(e).when(mockPubSubIPCEventStreamAgent).publish(any(), any(), any());

        PubSubPublisher pubSubPublisher = new PubSubPublisher(mockPubSubIPCEventStreamAgent);
        String sampleJson = readJsonFromFile(SAMPLE_RAW_METRICS_JSON);
        pubSubPublisher.publishMessage(sampleJson, DEFAULT_TELEMETRY_PUBSUB_TOPIC);

        verify(mockPubSubIPCEventStreamAgent, times(1)).publish(DEFAULT_TELEMETRY_PUBSUB_TOPIC,
                sampleJson.getBytes(StandardCharsets.UTF_8), AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER);
    }
}