<!-- SPDX-License-Identifier: CC-BY-4.0 -->
<!-- Copyright Contributors to the Egeria project. -->
# atlas-docker
Apache Atlas Dockerfile to support ongoing Egeria development work. It is not suitable as a general Atlas container as it needs to be much more configurable to integrate into the broader environment.

* Tested with Apache Atlas 'master' as of 28 Feb 2023

# Building the image

```
docker build . -t atlas -f Dockerfile
```

This may take an hour or more as it will download the atlas source, build it, and then package ready to run.
It will also configure kafka to be usable from outside the container.

The build should be run on linux, amd64, with docker. arm64 will fail due to hardcoded architecture reference in the Dockerfile (easily fixed), but also fails when using podman.

## Launch

Podman or docker can be used, it
```
docker run -p 9026:9026 -p 9027:9027 -p 21000:21000 atlas
```

## Access WebUI

Go to http://localhost:21000

## Creating a simple entity 

Click on 'create entity' and just create something simple. See [docs](https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.5/using-atlas/content/manually_creating_entities.html)

## Using Kafka

Integration via Kafka is interesting for egeria, in addition to the webui/endpoint

For example, using the tools provided in the 'kafka' package (add .sh to the commands when running on linux, whilst homebrew macOS users should use this version)

```
➜  atlas-docker git:(main) ✗ kafka-topics --list --bootstrap-server localhost:9027                                                   <<<
ATLAS_ENTITIES
ATLAS_HOOK
__consumer_offsets
```
Note that the ATLAS_ENTITIES topic is only created on first use

To monitor entities are they are created:
```
➜  atlas-docker git:(main) ✗ kafka-console-consumer --bootstrap-server localhost:9027 --topic ATLAS_ENTITIES --from-beginning
{"source":{},"version":{"version":"1.0.0","versionParts":[1]},"msgCompressionKind":"NONE","msgSplitIdx":1,"msgSplitCount":1,"msgSourceIP":"172.17.0.2","msgCreatedBy":"","msgCreationTime":1677621506752,"spooled":false,"message":{"type":"ENTITY_NOTIFICATION_V2","entity":{"typeName":"hdfs_path","attributes":{"path":"path","createTime":1677542400000,"qualifiedName":"hdfs.name","name":"hdfs name"},"guid":"6b7d094d-9ece-4be0-8b97-761aa1081abd","displayText":"hdfs name","isIncomplete":false},"operationType":"ENTITY_CREATE","eventTime":1677621506442}}
```

