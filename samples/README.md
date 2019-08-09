<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright Contributors to the ODPi Egeria project. -->

# Egeria Atlas Connector.postman_environment.json

Provides an example environment definition, with all the variables needed pre-defined (you simply need to provide values specific to your own environment).

# Egeria Atlas repo connector.postman_collection.json

This script can be used to configure Egeria for use with an existing Apache Atlas environment.

Prerequisites:

- an existing Apache Atlas environment, ideally running v2
- Kafka running and connected to the Apache Atlas environment (eg. the embedded Kafka)

Variables:

- `baseURL` the egeria URL
- `user` the userName to pass to Egeria
- `server` the server name for Egeria
- `cohort` the name of the cohort: used as the Kafka topic name for OMRS
- `kafkaep` Kafka endpoint for the cohort
- `atlas_host` the hostname (or IP address) of the existing Apache Atlas environment (web tier)
- `atlas_port` the port number of the web console of the existing Apache Atlas environment
- `atlas_user` the username of a user to access Atlas's REST API
- `atlas_password` the password of the user to access Atlas's REST API
- `atlas_kafka` the hostname (or IP address) and port of Atlas's internal Kafka bus (`atlas_host:9027` by default)

Each step is sequentially numbered so that they can be executed in-order as part of a Postman "Runner", if desired.

# Egeria Atlas READ tests.postman_collection.json

This script can be used to run through a number of different tests of the connector, assuming the Apache Atlas
environment has first been populated with the Glossary samples provided and those included in the Hortonworks Sandbox.

Prerequisites:

- an existing Apache Atlas environment, ideally running v2
- samples loaded
- connector configured (eg. using `Egeria Atlas repo connector.postman_collection.json` above)

----
License: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/),
Copyright Contributors to the ODPi Egeria project.
