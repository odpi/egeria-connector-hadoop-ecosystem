/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.topic_integrator.atlas.ebi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.atlas.model.notification.EntityNotification;
import org.odpi.openmetadata.frameworks.auditlog.AuditLog;
import org.odpi.openmetadata.repositoryservices.connectors.openmetadatatopic.OpenMetadataTopicListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Data Engine in topic processor is listening events from external data engines about
 * metadata changes. It will handle different types of events defined in Data Engine OMAS API module.
 */
public class EbiAtlasTopicListener implements OpenMetadataTopicListener {
    private static final Logger log = LoggerFactory.getLogger(EbiAtlasTopicListener.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final AuditLog auditLog;

      public EbiAtlasTopicListener(AuditLog auditLog) {
          this.auditLog = auditLog;
      }


    /**
     * Method to pass an event received on topic.
     *
     * @param atlasEvent inbound event
     */
    @Override
    public void processEvent(String atlasEvent) {
        log.debug("Processing instance event {}", atlasEvent);

        if (atlasEvent == null) {
            log.debug("Null instance event - ignoring event");
        } else {
            log.debug("Instance event - " + atlasEvent);

            try {
                EntityNotification entityNotification = OBJECT_MAPPER.readValue(atlasEvent, EntityNotification.class);
                log.debug("entityNotification - " + entityNotification);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
//                                auditLog.logException("process Data Engine inTopic Event",
//                                                      DataEngineAuditCode.PROCESS_EVENT_EXCEPTION.getMessageDefinition(e.getMessage()), e);
            }

        }
    }
//
//            try {
//                DataEngineEventHeader dataEngineEventHeader = OBJECT_MAPPER.readValue(atlasEvent, DataEngineEventHeader.class);
//
//                if ((dataEngineEventHeader != null)) {
//                    switch (dataEngineEventHeader.getEventType()) {
//
//                        case DATA_ENGINE_REGISTRATION_EVENT:
//                            dataEngineEventProcessor.processDataEngineRegistrationEvent(atlasEvent);
//                            break;
//                        case LINEAGE_MAPPINGS_EVENT:
//                            dataEngineEventProcessor.processLineageMappingsEvent(atlasEvent);
//                            break;
//                        case PORT_ALIAS_EVENT:
//                            dataEngineEventProcessor.processPortAliasEvent(atlasEvent);
//                            break;
//                        case PORT_IMPLEMENTATION_EVENT:
//                            dataEngineEventProcessor.processPortImplementationEvent(atlasEvent);
//                            break;
//                        case PROCESS_EVENT:
//                            dataEngineEventProcessor.processProcessEvent(atlasEvent);
//                            break;
//                        case SCHEMA_TYPE_EVENT:
//                            dataEngineEventProcessor.processSchemaTypeEvent(atlasEvent);
//                            break;
//                        case PROCESS_HIERARCHY_EVENT:
//                            dataEngineEventProcessor.processProcessHierarchyEvent(atlasEvent);
//                            break;
//                        case DELETE_PROCESS_EVENT:
//                            dataEngineEventProcessor.processDeleteProcessEvent(atlasEvent);
//                            break;
//                        case DELETE_PORT_IMPLEMENTATION_EVENT:
//                            dataEngineEventProcessor.processDeletePortImplementationEvent(atlasEvent);
//                            break;
//                        case DELETE_PORT_ALIAS_EVENT:
//                            dataEngineEventProcessor.processDeletePortAliasEvent(atlasEvent);
//                            break;
//                        case DELETE_SCHEMA_TYPE_EVENT:
//                            dataEngineEventProcessor.processDeleteSchemaTypeEvent(atlasEvent);
//                            break;
//                        case DELETE_DATA_ENGINE_EVENT:
//                            dataEngineEventProcessor.processDeleteDataEngineEvent(atlasEvent);
//                            break;
//                        case DATABASE_EVENT:
//                            dataEngineEventProcessor.processDatabaseEvent(atlasEvent);
//                            break;
//                        case DATABASE_SCHEMA_EVENT:
//                            dataEngineEventProcessor.processDatabaseSchemaEvent(atlasEvent);
//                            break;
//                        case RELATIONAL_TABLE_EVENT:
//                            dataEngineEventProcessor.processRelationalTableEvent(atlasEvent);
//                            break;
//                        case DATA_FILE_EVENT:
//                            dataEngineEventProcessor.processDataFileEvent(atlasEvent);
//                            break;
//                        case DELETE_DATABASE_EVENT:
//                            dataEngineEventProcessor.processDeleteDatabaseEvent(atlasEvent);
//                            break;
//                        case DELETE_DATABASE_SCHEMA_EVENT:
//                            dataEngineEventProcessor.processDeleteDatabaseSchemaEvent(atlasEvent);
//                            break;
//                        case DELETE_RELATIONAL_TABLE_EVENT:
//                            dataEngineEventProcessor.processDeleteRelationalTableEvent(atlasEvent);
//                            break;
//                        case DELETE_DATA_FILE_EVENT:
//                            dataEngineEventProcessor.processDeleteDataFileEvent(atlasEvent);
//                            break;
//                        case DELETE_FOLDER_EVENT:
//                            dataEngineEventProcessor.processDeleteFolderEvent(atlasEvent);
//                            break;
//                        case DELETE_CONNECTION_EVENT:
//                            dataEngineEventProcessor.processDeleteConnectionEvent(atlasEvent);
//                            break;
//                        case DELETE_ENDPOINT_EVENT:
//                            dataEngineEventProcessor.processDeleteEndpointEvent(atlasEvent);
//                            break;
//                        default:
//                            log.debug("Ignored instance event - unknown event type");
//                            break;
//                    }
//                } else {
//                    log.debug("Ignored instance event - null Data Engine event type");
//                }
//            } catch (JsonProcessingException e) {
//                log.debug("Exception processing event from in Data Engine In Topic", e);
//
//                auditLog.logException("process Data Engine inTopic Event",
//                                      DataEngineAuditCode.PROCESS_EVENT_EXCEPTION.getMessageDefinition(e.getMessage()), e);
//
//            }

}
