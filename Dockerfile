# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the Egeria project

# This is the EGERIA version - typically passed from the ci/cd pipeline
ARG EGERIA_BASE_IMAGE=quay.io/odpi/egeria:latest
ARG EGERIA_VERSION=latest
# Must be set to help get the right files for the connextors

FROM ${EGERIA_BASE_IMAGE}:${EGERIA_VERSION}
ARG CONNECTOR_VERSION=0.1-SNAPSHOT

# Labels from https://github.com/opencontainers/image-spec/blob/master/annotations.md#pre-defined-annotation-keys (with additions prefixed    ext)
# We should inherit all the base labels from the egeria image and only overwrite what is necessary.
LABEL org.opencontainers.image.description = "Egeria with Atlas repository connector" \
      org.opencontainers.image.documentation = "https://github.com/odpi/egeria-connector-hadoop-ecosystem"

# Note that we currently only build a simple jar. If additional dependencies are needed, a jar with dependencies will be needed, or the libraries will need to be added here
ENV CONNECTOR_VERSION ${CONNECTOR_VERSION}

COPY apache-atlas-adapter/build/libs/egeria-connector-apache-atlas-adapter-${CONNECTOR_VERSION}-jar-with-dependencies.jar /deployments/server/lib

