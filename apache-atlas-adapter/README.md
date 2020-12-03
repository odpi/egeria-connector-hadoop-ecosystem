<!-- SPDX-License-Identifier: CC-BY-4.0 -->
<!-- Copyright Contributors to the ODPi Egeria project. -->

[![javadoc](https://javadoc.io/badge2/org.odpi.egeria/egeria-connector-apache-atlas-adapter/javadoc.svg)](https://javadoc.io/doc/org.odpi.egeria/egeria-connector-apache-atlas-adapter) [![Maven Central](https://img.shields.io/maven-central/v/org.odpi.egeria/egeria-connector-apache-atlas-adapter)](https://mvnrepository.com/artifact/org.odpi.egeria/egeria-connector-apache-atlas-adapter)

# Apache Atlas connector

[Apache Atlas](https://atlas.apache.org) is an open source metadata repository. This connector provides an example
implementation for interacting with a metadata repository through the open metadata standards of Egeria.

Note that currently the implemented connector is read-only: it only implements those methods necessary to search, retrieve,
and communicate metadata from Atlas out into the cohort -- it does *not* currently implement the ability to update Atlas
based on events received from other members of the cohort. (This is due to some current limitations in Apache Atlas --
see [open issues](https://github.com/odpi/egeria-connector-apache-atlas/issues?q=is%3Aissue+is%3Aopen+label%3Aexternal).)

Furthermore, [only a subset of the overall Open Metadata Types are currently implemented](../docs/mappings/README.md).

## How it works

The Apache Atlas Repository Connector works through a combination of the following:

- Apache Atlas's REST API, itself abstracted through the Apache Atlas Java Client
- Apache Atlas's embedded Apache Kafka event bus
    - specifically the `ATLAS_ENTITIES` topic

## Getting started

### TL;DR

The quick version:

1. Start with the TL;DR instructions on the [main page](../README.md).
1. In another shell / command-line window, run the following commands to configure Egeria and startup its services --
   making sure to replace the hostnames and port numbers with those relevant to your own environment (`localhost:9092`
   for your own Kafka bus, `atlas:9027` with the Atlas-embedded Kafka host and port, `atlas` with
   the hostname of your Apache Atlas server, `21000` with the port number of your Apache Atlas server,
   `admin` with the username for your Apache Atlas environment, and `admin` with the password for your Apache Atlas
   environment):
    ```bash
    $ curl -X POST -H "Content-Type: application/json" --data '{"producer":{"bootstrap.servers":"localhost:9092"},"consumer":{"bootstrap.servers":"localhost:9092"}}' "https://localhost:9443/open-metadata/admin-services/users/admin/servers/myserver/event-bus?connectorProvider=org.odpi.openmetadata.adapters.eventbus.topic.kafka.KafkaOpenMetadataTopicProvider&topicURLRoot=OMRSTopic"
    $ curl -X POST "https://localhost:9443/open-metadata/admin-services/users/admin/servers/myserver/cohorts/mycohort"
    $ curl -X POST -H "Content-Type: application/json" --data '{"class":"Connection","connectorType":{"class":"ConnectorType","connectorProviderClassName":"org.odpi.egeria.connectors.apache.atlas.repositoryconnector.ApacheAtlasOMRSRepositoryConnectorProvider"},"endpoint":{"class":"Endpoint","address":"atlas:21000","protocol":"http"},"userId":"admin","clearPassword":"admin"}' "https://localhost:9443/open-metadata/admin-services/users/admin/servers/myserver/local-repository/mode/repository-proxy/connection"
    $ curl -X POST "https://localhost:9443/open-metadata/admin-services/users/admin/servers/myserver/local-repository/event-mapper-details?connectorProvider=org.odpi.egeria.connectors.apache.atlas.eventmapper.ApacheAtlasOMRSRepositoryEventMapperProvider&eventSource=atlas:9027"
    $ curl -X POST "https://localhost:9443/open-metadata/admin-services/users/admin/servers/myserver/instance"
    ```

### Detailed steps for configuring the Apache Atlas connector

You will need to configure the OMAG Server Platform as follows (order is important) to make use of this Egeria connector.
For example payloads and endpoints, see the [Postman samples](../samples).

1. To start using the connector, you will need an Apache Atlas environment, ideally running version 2. You will need to
    first enable all event notifications (ie. including for relationships) in your Apache Atlas environment, by adding
    the following line to your `conf/atlas-application.properties` file:

    ```properties
    atlas.notification.relationships.enabled=true
    ```

    You will likely need to restart your environment after making this change.

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
    POST https://localhost:9443/open-metadata/admin-services/users/admin/servers/myserver/event-bus?connectorProvider=org.odpi.openmetadata.adapters.eventbus.topic.kafka.KafkaOpenMetadataTopicProvider&topicURLRoot=OMRSTopic
    ```

1. Configure the cohort, by POSTing something like the following:

    ```
    POST https://localhost:9443/open-metadata/admin-services/users/admin/servers/myserver/cohorts/mycohort
    ```

1. Configure the Apache Atlas connector, by POSTing a payload like the following, replacing the `{{atlas_host}}` with
   the hostname of your Apache Atlas instance, `{{atlas_port}}` with the port number of it, `{{atlas_user}}` with the
   username of a user able to access the REST API (eg. `admin`), and `{{atlas_password}}` with the password for that
   user:

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
    POST https://localhost:9443/open-metadata/admin-services/users/admin/servers/myserver/local-repository/mode/repository-proxy/connection
    ```

   Note that you also need to provide the `connectorProviderClassName` parameter, set to the name of the Apache Atlas
   connectorProvider class (value as given above).

1. Configure the event mapper for Apache Atlas, by POSTing something like the following:

    ```
    POST https://localhost:9443/open-metadata/admin-services/users/admin/servers/myserver/local-repository/event-mapper-details?connectorProvider=org.odpi.egeria.connectors.apache.atlas.eventmapper.ApacheAtlasOMRSRepositoryEventMapperProvider&eventSource=my.atlas.host.com:9027
    ```

   The hostname provided at the end should be the host on which your Apache Atlas Kafka bus is running, and include
   the appropriate port number for connecting to that bus. (By default, for the embedded Kafka bus, the port is `9027`.)

1. The connector and event mapper should now be configured, and you should now be able
   to start the instance by POSTing something like the following:

   ```
   POST https://localhost:9443/open-metadata/admin-services/users/admin/servers/myserver/instance
   ```

After following these instructions, your Apache Atlas instance will be participating in the Egeria cohort. For those
objects supported by the connector, most new instances or updates to existing instances should result in that metadata
automatically being communicated out to the rest of the cohort.

(Note: there are still some limitations with Apache Atlas that prevent this being true for _all_ types, eg. see
[Jira ATLAS-3312](https://issues.apache.org/jira/projects/ATLAS/issues/ATLAS-3312))

## Loading samples

If you have a completely empty Apache Atlas environment, you may want to load some sample metadata to further explore.

Samples are provided under [samples](../samples):

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

