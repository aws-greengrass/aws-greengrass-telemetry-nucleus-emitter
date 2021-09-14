/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter.publisher;

import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqttclient.MqttClient;
import com.aws.greengrass.mqttclient.PublishRequest;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.crt.mqtt.QualityOfService;

import java.nio.charset.StandardCharsets;
import javax.inject.Inject;

import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MQTT_PUBLISH_FAILURE_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.MQTT_PUBLISH_SUCCESS_LOG;

@RequiredArgsConstructor(access = AccessLevel.PUBLIC, onConstructor = @__({ @Inject}))
public class MqttPublisher implements TelemetryPublisher {

    private final MqttClient mqttClient;
    private static final Logger logger = LogManager.getLogger(MqttPublisher.class);

    /**
     * Publish telemetry to AWS IoT Core on the configured MQTT topic.
     * We use QoS0 to not burden the spooler when offline.
     *
     * @param message Message containing telemetry payload.
     * @param topic AWS IoT MQTT topic to publish to.
     */
    @Override
    public void publishMessage(String message, String topic) {
            if (!this.mqttClient.connected()) {
                return;
            }
            PublishRequest publishRequest = PublishRequest.builder()
                    .qos(QualityOfService.AT_MOST_ONCE)
                    .topic(topic)
                    .payload(message.getBytes(StandardCharsets.UTF_8)).build();

            this.mqttClient.publish(publishRequest)
                    .whenComplete((msg, ex) -> {
                        if (ex == null) {
                            logger.trace(MQTT_PUBLISH_SUCCESS_LOG, topic);
                        } else {
                            logger.error(MQTT_PUBLISH_FAILURE_LOG, topic, ex);
                        }
                    });
    }
}