/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter.publisher;

import com.aws.greengrass.builtin.services.pubsub.PubSubIPCEventStreamAgent;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import software.amazon.awssdk.aws.greengrass.model.InvalidArgumentsError;

import java.nio.charset.StandardCharsets;
import javax.inject.Inject;

import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.PUBSUB_PUBLISH_FAILURE_LOG;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.PUBSUB_PUBLISH_SUCCESS_LOG;

@RequiredArgsConstructor(access = AccessLevel.PUBLIC, onConstructor = @__({ @Inject}))
public class PubSubPublisher implements TelemetryPublisher {

    private final PubSubIPCEventStreamAgent pubSubIPCEventStreamAgent;
    private static final Logger logger = LogManager.getLogger(PubSubPublisher.class);

    /**
     * Publish telemetry to local pub/sub on the default local telemetry pub/sub topic.
     *
     * @param message Message containing telemetry payload.
     * @param topic Local pub/sub topic to publish to.
     */
    @Override
    public void publishMessage(String message, String topic) {
        try {
            this.pubSubIPCEventStreamAgent.publish(topic, message.getBytes(StandardCharsets.UTF_8),
                    AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER);
            logger.trace(PUBSUB_PUBLISH_SUCCESS_LOG, topic);
        } catch (InvalidArgumentsError e) {
            logger.error(PUBSUB_PUBLISH_FAILURE_LOG, topic, e);
        }
    }
}


