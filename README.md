<!-- SPDX-License-Identifier: CC-BY-4.0 -->
<!-- Copyright Contributors to the ODPi Egeria project. -->

[![GitHub](https://img.shields.io/github/license/odpi/egeria-connector-apache-atlas)](LICENSE) [![Azure](https://dev.azure.com/odpi/egeria/_apis/build/status/odpi.egeria-connector-apache-atlas)](https://dev.azure.com/odpi/Egeria/_build) [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=egeria-connector-apache-atlas&metric=alert_status)](https://sonarcloud.io/dashboard?id=egeria-connector-apache-atlas)

# Apache Atlas Repository Connector

[Apache Atlas](https://atlas.apache.org) is an open source metadata repository. This connector provides an example
implementation for interacting with a metadata repository through the open metadata standards of Egeria.

Note that currently the implemented connector is read-only: it only implements those methods necessary to search, retrieve,
and communicate metadata from Atlas out into the cohort -- it does *not* currently implement the ability to update Atlas
based on events received from other members of the cohort. (This is due to some current limitations in Apache Atlas -- 
see [open issues](https://github.com/odpi/egeria-connector-apache-atlas/issues?q=is%3Aissue+is%3Aopen+label%3Aexternal).)

Furthermore, [only a subset of the overall Open Metadata Types are currently implemented](docs/mappings/README.md).

## How it works

The Apache Atlas Repository Connector works through a combination of the following:

- Apache Atlas's REST API, itself abstracted through the Apache Atlas Java Client
- Apache Atlas's embedded Apache Kafka event bus
    - specifically the `ATLAS_ENTITIES` topic

## Getting started

### Enable Apache Atlas's events

To start using the connector, you will need an Apache Atlas environment, ideally running version 2. You will need to
first enable all event notifications (ie. including for relationships) in your Apache Atlas environment, by adding the
following line to your `conf/atlas-application.properties` file:

```properties
atlas.notification.relationships.enabled=true
```

You will likely need to restart your environment after making this change.

### Build connector and copy to OMAG Server Platform

After building the connector project (`mvn clean install`) the connector is available as:

```text
distribution/target/egeria-connector-apache-atlas-package-VERSION.jar
```

Simply copy this file to a location where it can be run alongside the OMAG Server
Platform from the Egeria core (in the example below, the file would be copied to
`/lib/egeria-connector-apache-atlas-package-VERSION.jar`).

### Configure security

There are [multiple options to configure the security of your environment](docs/security/README.md) for this connector,
but this must be done prior to starting up the connector itself (step below).

For simple tests, if you can run your Apache Atlas environment with only its most basic security and without HTTPS, then
there is nothing additional you need to configure for the connector.

Alternatively, if you can still use basic authentication (username and password) but must run Apache Atlas via HTTPS,
and you simply want to test things out, the simplest (but most insecure) option is to set the environment variable
`STRICT_SSL` to `false` using something like the following prior to starting up the OMAG Server Platform:

```bash
export STRICT_SSL=false
```

Note that this will disable all certificate validation for SSL connections made between Egeria and your Apache Atlas
environment, so is inherently insecure.

### Startup the OMAG Server Platform

You can startup the OMAG Server Platform with this connector ready-to-be-configured by running:

```bash
$ java -Dloader.path=/lib -jar server-chassis-spring-VERSION.jar
```

(This command will startup the OMAG Server Platform, including all libraries
in the `/lib` directory as part of the classpath of the OMAG Server Platform.)

### Configure this Egeria connector

You will need to configure the OMAG Server Platform as follows (order is important) to make use of this Egeria connector.
For example payloads and endpoints, see the [Postman samples](samples).

1. Configure your event bus for Egeria, by POSTing a payload like the following:

    ```json
    {
        "producer": {
            "bootstrap.servers":"localhost:9092"
        },
        "consumer": {
            "bootstrap.servers":"localhost:9092"
        }
    }
    ```

    to:

    ```
    POST http://localhost:8080/open-metadata/admin-services/users/{{user}}/servers/{{server}}/event-bus?connectorProvider=org.odpi.openmetadata.adapters.eventbus.topic.kafka.KafkaOpenMetadataTopicProvider&topicURLRoot=OMRSTopic
    ```

1. Configure the cohort, by POSTing something like the following:

    ```
    POST http://localhost:8080/open-metadata/admin-services/users/{{user}}/servers/{{server}}/cohorts/cocoCohort
    ```

1. Configure the Apache Atlas connector, by POSTing a payload like the following:

    ```json
    {
        "class": "Connection",
        "connectorType": {
            "class": "ConnectorType",
            "connectorProviderClassName": "org.odpi.egeria.connectors.apache.atlas.repositoryconnector.ApacheAtlasOMRSRepositoryConnectorProvider"
        },
        "endpoint": {
            "class": "Endpoint",
            "address": "{{atlas_host}}:{{atlas_port}}",
            "protocol": "http"
        },
        "userId": "{{atlas_user}}",
        "clearPassword": "{{atlas_password}}"
    }
    ```

    to:

    ```
    {{baseURL}}/open-metadata/admin-services/users/{{user}}/servers/{{server}}/local-repository/mode/repository-proxy/connection
    ```

    The payload should include the hostname and port of your Apache Atlas environment, and a `username` and `password`
    through which the REST API can be accessed.

    Note that you also need to provide the `connectorProviderClassName` parameter, set to the name of the Apache Atlas
    connectorProvider class (value as given above).

1. Configure the event mapper for Apache Atlas, by POSTing something like the following:

    ```
    POST http://localhost:8080/open-metadata/admin-services/users/{{user}}/servers/{{server}}/local-repository/event-mapper-details?connectorProvider=org.odpi.egeria.connectors.apache.atlas.eventmapper.ApacheAtlasOMRSRepositoryEventMapperProvider&eventSource=my.atlas.host.com:9027
    ```

    The hostname provided at the end should be the host on which your Apache Atlas Kafka bus is running, and include
    the appropriate port number for connecting to that bus. (By default, for the embedded Kafka bus, the port is `9027`.)

1. The connector and event mapper should now be configured, and you should now be able
    to start the instance by POSTing something like the following:

   ```
   POST http://localhost:8080/open-metadata/admin-services/users/{{user}}/servers/{{server}}/instance
   ```

After following these instructions, your Apache Atlas instance will be participating in the Egeria cohort. For those
objects supported by the connector, most new instances or updates to existing instances should result in that metadata
automatically being communicated out to the rest of the cohort.

(Note: there are still some limitations with Apache Atlas that prevent this being true for _all_ types, eg. see 
[Jira ATLAS-3312](https://issues.apache.org/jira/projects/ATLAS/issues/ATLAS-3312?filter=reportedbyme))

## Loading samples

If you have a completely empty Apache Atlas environment, you may want to load some sample metadata to further explore.

Samples are provided under [samples](samples):

- `AtlasGlossary.zip` contains a sample glossary, set of categories and terms as defined in the [Coco Pharmaceuticals](https://github.com/odpi/egeria/tree/master/open-metadata-resources/open-metadata-deployment/sample-data/coco-pharmaceuticals)
    set of samples.

These can be loaded to the environment using the following command:

```shell script
$ curl -g -X POST -u <user>:<password> -H "Content-Type: multipart/form-data" -H "Cache-Control: no-cache" -F data=@AtlasGlossary.zip "http://<host>:<port>/api/atlas/admin/import"
```

For additional samples, eg. of Hadoop-native entity types, see the [Hortonworks Sanbox](https://www.cloudera.com/downloads/hortonworks-sandbox.html).


----
License: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/),
Copyright Contributors to the ODPi Egeria project.

