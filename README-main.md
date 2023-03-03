<!-- SPDX-License-Identifier: CC-BY-4.0 -->
<!-- Copyright Contributors to the ODPi Egeria project. -->

[![License](https://img.shields.io/github/license/odpi/egeria-connector-hadoop-ecosystem)](LICENSE)
[![Build](https://github.com/odpi/egeria-connector-hadoop-ecosystem/workflows/Build/badge.svg)](https://github.com/odpi/egeria-connector-hadoop-ecosystem/actions/workflows/merge.yml?query=workflow%3ABuild)
[![CodeQL](https://github.com/odpi/egeria-connector-hadoop-ecosystem/workflows/CodeQL/badge.svg)](https://github.com/odpi/egeria-connector-hadoop-ecosystem/actions/workflows/codeql-analysis.yml)
[![Release](https://img.shields.io/maven-central/v/org.odpi.egeria/egeria-connector-hadoop-ecosystem-package?label=release)](http://repository.sonatype.org/service/local/artifact/maven/redirect?r=central-proxy&g=org.odpi.egeria&a=egeria-connector-apache-atlas-adaptere&v=RELEASE&c=jar-with-dependencies)
[![Development](https://img.shields.io/nexus/s/org.odpi.egeria/egeria-connector-hadoop-ecosystem-package?label=development&server=https%3A%2F%2Foss.sonatype.org)](https://oss.sonatype.org/content/repositories/snapshots/org/odpi/egeria/egeria-connector-apache-atlas-adapter/)

# Hadoop Ecosystem Repository Connectors

This repository houses the ODPi Egeria connectors for:

- [Apache Atlas connector](apache-atlas-adapter) implements read-only connectivity to the Apache Atlas metadata repository. A [Dockerfile](test/containers/atlas/Dockerfile) is also provided to help you get started with Apache Atlas for demos & testing.

## Quick links


- See the [CTS Results](cts/README.md) for details on its conformance.
- See the [detailed readme](apache-atlas-adapter/README.md) configuration instructions, and information on how to test with Atlas using provided Dockerfile

Note the [Getting Started](https://odpi.github.io/egeria-connector-hadoop-ecosystem/getting-started/index.html) guide needs updating so please use the development docs here for now.
## How it works

There is currently only an Apache Atlas connector. This connector integrates into the Open Connector Framework (OCF) and
implements certain methods that adhere to the interfaces required by a repository proxy. These then communicate with
Apache Atlas via Apache Atlas's Java client (in turn communicating via Apache Atlas's REST API) to read information
from an Apache Atlas environment.

This requires an existing Apache Atlas environment to already be running elsewhere: the connector simply connects to
this existing environment and "proxies" any requests from Egeria into a series of native API calls against the environment.

![Overview](docs/overview.png)

> Overview of the connector implementation

Note that the connector operates only in a read-only manner: no write operations (creates or updates) are implemented
against Apache Atlas. Furthermore, [only a subset of the open metadata types are currently mapped](docs/mappings/README.md).

----
License: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/),
Copyright Contributors to the ODPi Egeria project.

