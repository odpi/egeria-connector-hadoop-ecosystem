/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.eventmapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.ApacheAtlasOMRSMetadataCollection;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.ApacheAtlasOMRSRepositoryConnector;
import org.odpi.openmetadata.frameworks.connectors.Connector;
import org.odpi.openmetadata.frameworks.connectors.VirtualConnectorExtension;
import org.odpi.openmetadata.frameworks.connectors.ffdc.ConnectorCheckedException;
import org.odpi.openmetadata.repositoryservices.auditlog.OMRSAuditCode;
import org.odpi.openmetadata.repositoryservices.connectors.openmetadatatopic.OpenMetadataTopicConnector;
import org.odpi.openmetadata.repositoryservices.connectors.openmetadatatopic.OpenMetadataTopicListener;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryConnector;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryeventmapper.OMRSRepositoryEventMapperBase;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.RepositoryErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * ApacheAtlasOMRSRepositoryEventMapper supports the event mapper function for Apache Atlas
 * when used as an open metadata repository.
 */
public class ApacheAtlasOMRSRepositoryEventMapper extends OMRSRepositoryEventMapperBase
        implements VirtualConnectorExtension, OpenMetadataTopicListener {

    private List<Connector> embeddedConnectors = null;
    private List<OpenMetadataTopicConnector> eventBusConnectors = new ArrayList<>();

    private static final Logger log = LoggerFactory.getLogger(ApacheAtlasOMRSRepositoryEventMapper.class);

    private String sourceName;
    private ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector;
    private ApacheAtlasOMRSMetadataCollection atlasMetadataCollection;
    private String metadataCollectionId;
    private String originatorServerName;
    private String originatorServerType;

    private Properties atlasKafkaProperties;
    private String atlasKafkaBootstrap;
    private String atlasKafkaTopic;

    private ObjectMapper mapper;

    /**
     * Default constructor
     */
    public ApacheAtlasOMRSRepositoryEventMapper() {
        super();
        this.sourceName = "ApacheAtlasOMRSRepositoryEventMapper";
    }


    /**
     * Pass additional information to the connector needed to process events.
     *
     * @param repositoryEventMapperName repository event mapper name used for the source of the OMRS events.
     * @param repositoryConnector ths is the connector to the local repository that the event mapper is processing
     *                            events from.  The repository connector is used to retrieve additional information
     *                            necessary to fill out the OMRS Events.
     */
    @Override
    public void initialize(String                  repositoryEventMapperName,
                           OMRSRepositoryConnector repositoryConnector) {

        super.initialize(repositoryEventMapperName, repositoryConnector);
        log.info("Apache Atlas Event Mapper initializing...");

        // Setup Apache Atlas Repository connectivity
        this.atlasRepositoryConnector = (ApacheAtlasOMRSRepositoryConnector) this.repositoryConnector;
        this.atlasKafkaTopic = "ATLAS_ENTITIES";

        // Retrieve connection details to configure Kafka connectivity
        this.atlasKafkaBootstrap = this.connectionBean.getEndpoint().getAddress();
        atlasKafkaProperties = new Properties();
        atlasKafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, atlasKafkaBootstrap);
        atlasKafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "ApacheAtlasOMRSRepositoryEventMapper_consumer");
        atlasKafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        atlasKafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Setup ObjectMapper for (de-)serialisation of events
        this.mapper = new ObjectMapper();
        this.mapper.enableDefaultTyping();

    }


    /**
     * Indicates that the connector is completely configured and can begin processing.
     *
     * @throws ConnectorCheckedException there is a problem within the connector.
     */
    @Override
    public void start() throws ConnectorCheckedException {

        super.start();
        log.info("Apache Atlas Event Mapper starting...");
        try {
            this.atlasMetadataCollection = (ApacheAtlasOMRSMetadataCollection) atlasRepositoryConnector.getMetadataCollection();
        } catch (RepositoryErrorException e) {
            throw new ConnectorCheckedException(
                    e.getReportedHTTPCode(),
                    this.getClass().getCanonicalName(),
                    e.getReportingActionDescription(),
                    e.getErrorMessage(),
                    e.getReportedSystemAction(),
                    e.getReportedUserAction(),
                    e
            );
        }
        this.metadataCollectionId = atlasRepositoryConnector.getMetadataCollectionId();
        this.originatorServerName = atlasRepositoryConnector.getServerName();
        this.originatorServerType = atlasRepositoryConnector.getServerType();

        final String  methodName = "start";

        /*
         * Step through the embedded connectors, selecting only the OpenMetadataTopicConnectors
         * to use.
         */
        if (embeddedConnectors != null) {

            for (Connector  embeddedConnector : embeddedConnectors) {
                if (embeddedConnector instanceof OpenMetadataTopicConnector) {
                    /*
                     * Successfully found an event bus connector of the right type.
                     */
                    OpenMetadataTopicConnector realTopicConnector = (OpenMetadataTopicConnector)embeddedConnector;

                    String   topicName = realTopicConnector.registerListener(this);
                    this.eventBusConnectors.add(realTopicConnector);

                    if (auditLog != null) {
                        OMRSAuditCode auditCode = OMRSAuditCode.EVENT_MAPPER_LISTENER_REGISTERED;
                        auditLog.logRecord(methodName,
                                auditCode.getLogMessageId(),
                                auditCode.getSeverity(),
                                auditCode.getFormattedLogMessage(repositoryEventMapperName, topicName),
                                this.getConnection().toString(),
                                auditCode.getSystemAction(),
                                auditCode.getUserAction());
                    }
                }
            }
        }

        /*
         * OMRSTopicConnector needs at least one event bus connector to operate successfully.
         */
        if (this.eventBusConnectors.isEmpty() && auditLog != null) {
            OMRSAuditCode auditCode = OMRSAuditCode.EVENT_MAPPER_LISTENER_DEAF;
            auditLog.logRecord(methodName,
                    auditCode.getLogMessageId(),
                    auditCode.getSeverity(),
                    auditCode.getFormattedLogMessage(repositoryEventMapperName),
                    this.getConnection().toString(),
                    auditCode.getSystemAction(),
                    auditCode.getUserAction());
        }

        log.info("Starting consumption from Apache Atlas Kafka bus.");
        new Thread(new KafkaConsumerThread()).start();

    }


    /**
     * Class to support multi-threaded consumption of Apache Atlas Kafka events.
     */
    private class KafkaConsumerThread implements Runnable {

        /**
         * Read Apache Atlas Kafka events.
         */
        @Override
        public void run() {

            log.info("Starting Apache Atlas Event Mapper consumer thread.");
            final Consumer<Long, String> consumer = new KafkaConsumer<>(atlasKafkaProperties);
            consumer.subscribe(Collections.singletonList(atlasKafkaTopic));

            while (true) {
                try {
                    ConsumerRecords<Long, String> events = consumer.poll(100);
                    for (ConsumerRecord<Long, String> event : events) {
                        processEvent(event.value());
                    }
                } catch (Exception e) {
                    log.error("Failed trying to consume Apache Atlas events from Kafka.", e);
                }
            }
        }

    }


    /**
     * Registers itself as a listener of any OpenMetadataTopicConnectors that are passed as
     * embedded connectors.
     *
     * @param embeddedConnectors  list of connectors
     */
    @Override
    public void initializeEmbeddedConnectors(List<Connector> embeddedConnectors) {
        this.embeddedConnectors = embeddedConnectors;
    }


    /**
     * Method to pass an event received on topic.
     *
     * @param event inbound event
     */
    @Override
    public void processEvent(String event) {
        if (log.isInfoEnabled()) { log.info("Processing event: {}", event); }
    }

}
