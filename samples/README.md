<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright Contributors to the ODPi Egeria project. -->

# Samples

These sample Postman collections illustrate configuring and using the Apache Atlas connector
for ODPi Egeria.

Each should be used with the
[environment defined in the Egeria Core samples](https://github.com/odpi/egeria/blob/master/open-metadata-resources/open-metadata-samples/postman-rest-samples/README.md),
which has all of the needed variables defined within it.

# Egeria-Apache-Atlas-config.postman_collection.json

This script can be used to configure Egeria for use with an existing Apache Atlas environment.

Prerequisites:

- an existing Apache Atlas environment, ideally running v2
- Kafka running and connected to the Apache Atlas environment (eg. the embedded Kafka)

Apache Atlas-specific variables:

- `atlas_host` the hostname (or IP address) of the existing Apache Atlas environment (web tier)
- `atlas_port` the port number of the web console of the existing Apache Atlas environment
- `atlas_user` the username of a user to access Atlas's REST API
- `atlas_password` the password of the user to access Atlas's REST API
- `atlas_kafka` the hostname (or IP address) and port of Atlas's internal Kafka bus (`atlas_host:9027` by default)

Each step is sequentially numbered so that they can be executed in-order as part of a Postman "Runner", if desired.

# Egeria-Apache-Atlas-read.postman_collection.json

This script can be used to run through a number of different tests of the connector, assuming the Apache Atlas
environment has first been populated with the Glossary samples provided and those included in the Hortonworks Sandbox.

Prerequisites:

- an existing Apache Atlas environment, ideally running v2
- samples loaded
- connector configured (eg. using `Egeria-Apache-Atlas-config.postman_collection.json` above)

----
License: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/),
Copyright Contributors to the ODPi Egeria project.
