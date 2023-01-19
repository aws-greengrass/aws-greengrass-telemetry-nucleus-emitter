/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.telemetry.nucleus.emitter;


import com.aws.greengrass.dependency.State;
import com.aws.greengrass.lifecyclemanager.GreengrassService;
import com.aws.greengrass.lifecyclemanager.Kernel;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER;
import static com.aws.greengrass.telemetry.nucleus.emitter.Constants.DEFAULT_TELEMETRY_PUBSUB_TOPIC;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class NucleusEmitterTestUtils {

    //Need to escape the $ symbol for matching
    public static final String REGEX_DEFAULT_TELEMETRY_PUBSUB_TOPIC = "\\" + DEFAULT_TELEMETRY_PUBSUB_TOPIC;
    public static final String TEST_MQTT_TOPIC = "test/topic";
    public static final String SAMPLE_RAW_METRICS_JSON = "sample_raw_metrics.json";
    public static final String SAMPLE_RAW_KERNEL_METRICS_JSON = "sample_raw_kernel_metrics.json";
    public static final String SAMPLE_RAW_SYSTEM_METRICS_JSON = "sample_raw_system_metrics.json";
    public static final String DEFAULT_NUCLEUS_EMITTER_KERNEL_CONFIG = "config_default.yaml";
    public static final String INVALID_THRESHOLD_NUCLEUS_EMITTER_KERNEL_CONFIG = "config_invalid_threshold.yaml";
    public static final String INVALID_PUBSUB_PUBLISH_NUCLEUS_EMITTER_KERNEL_CONFIG =
            "config_invalid_pubSubPublish.yaml";
    public static final String INVALID_MQTT_TOPIC_NUCLEUS_EMITTER_KERNEL_CONFIG =
            "config_invalid_mqttTopic.yaml";
    public static final String INVALID_TELEMETRY_PUBLISH_INTERVALMS_NUCLEUS_EMITTER_KERNEL_CONFIG =
            "config_invalid_telemetryPublishIntervalMs.yaml";
    public static final String NO_CONFIG_OPTIONS_NUCLEUS_EMITTER_KERNEL_CONFIG = "config_no_options.yaml";
    public static final String MQTT_NUCLEUS_EMITTER_KERNEL_CONFIG = "config_mqtt.yaml";


    public static String readJsonFromFile(String filename) throws IOException, URISyntaxException {
        File file = new File(NucleusEmitterTestUtils.class.getResource(filename).toURI());
        return new String(Files.readAllBytes(file.toPath()));
    }

    public static void startKernelWithConfig(String configFile, Kernel kernel, Path rootDir) throws InterruptedException {
        // Set this property for kernel to scan its own classpath to find plugins
        System.setProperty("aws.greengrass.scanSelfClasspath", "true");

        CountDownLatch nucleusTelemetryEmitterRunning = new CountDownLatch(1);
        kernel.parseArgs("-r", rootDir.toAbsolutePath().toString(), "-i", configFile);
        kernel.getContext().addGlobalStateChangeListener((GreengrassService service, State was, State newState) -> {
            if (service.getName().equals(AWS_GREENGRASS_TELEMETRY_NUCLEUS_EMITTER) && service.getState()
                    .equals(State.RUNNING)) {
                nucleusTelemetryEmitterRunning.countDown();
            }
        });
        kernel.launch();
        assertTrue(nucleusTelemetryEmitterRunning.await(5, TimeUnit.SECONDS));
    }

    public static String format(String original, String ... replacements) {
        for (String s : replacements) {
            original = original.replaceFirst(Pattern.quote("{}"), s);
        }
        return original;
    }

    private NucleusEmitterTestUtils() {
    }

}
