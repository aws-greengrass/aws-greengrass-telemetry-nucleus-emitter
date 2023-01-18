#!/bin/bash

set -e

COMPONENT_NAME='aws.greengrass.telemetry.NucleusEmitter'
VERSION='NEXT_PATCH'

mvn clean package -DskipTests

cp target/aws.greengrass.telemetry.NucleusEmitter.jar greengrass-build/artifacts/${COMPONENT_NAME}/${VERSION}/ && cp recipe.json greengrass-build/recipes/