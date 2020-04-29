/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.auditlog;

import org.odpi.openmetadata.frameworks.auditlog.messagesets.AuditLogMessageDefinition;
import org.odpi.openmetadata.frameworks.auditlog.messagesets.AuditLogMessageSet;
import org.odpi.openmetadata.repositoryservices.auditlog.OMRSAuditLogRecordSeverity;

import java.text.MessageFormat;

/**
 * The ApacheAtlasOMRSAuditCode is used to define the message content for the OMRS Audit Log.
 *
 * The 5 fields in the enum are:
 * <ul>
 *     <li>Log Message Id - to uniquely identify the message</li>
 *     <li>Severity - is this an event, decision, action, error or exception</li>
 *     <li>Log Message Text - includes placeholder to allow additional values to be captured</li>
 *     <li>Additional Information - further parameters and data relating to the audit message (optional)</li>
 *     <li>SystemAction - describes the result of the situation</li>
 *     <li>UserAction - describes how a user should correct the situation</li>
 * </ul>
 */
public enum ApacheAtlasOMRSAuditCode implements AuditLogMessageSet {

    REPOSITORY_SERVICE_STARTING("OMRS-ATLAS-REPOSITORY-0001",
            OMRSAuditLogRecordSeverity.INFO,
            "The Apache Atlas proxy is starting a new server instance",
            "The local server has started up a new instance of the Apache Atlas proxy.",
            "No action is required.  This is part of the normal operation of the service."),
    CONNECTING_TO_ATLAS("OMRS-ATLAS-REPOSITORY-0002",
            OMRSAuditLogRecordSeverity.INFO,
            "The Apache Atlas proxy is attempting to connect to Apache Atlas at {0}",
            "The local server is attempting to connect to the Apache Atlas server.",
            "No action is required.  This is part of the normal operation of the service."),
    CONNECTED_TO_ATLAS("OMRS-ATLAS-REPOSITORY-0003",
            OMRSAuditLogRecordSeverity.INFO,
            "The Apache Atlas proxy has successfully connected to Apache Atlas at {0}",
            "The local server has successfully connected to the Apache Atlas server.",
            "No action is required.  This is part of the normal operation of the service."),
    REPOSITORY_SERVICE_STARTED("OMRS-ATLAS-REPOSITORY-0004",
            OMRSAuditLogRecordSeverity.INFO,
            "The Apache Atlas proxy has started a new instance for server {0}",
            "The local server has completed startup of a new instance.",
            "No action is required.  This is part of the normal operation of the service."),
    REPOSITORY_SERVICE_SHUTDOWN("OMRS-ATLAS-REPOSITORY-0005",
            OMRSAuditLogRecordSeverity.INFO,
            "The Apache Atlas proxy has shutdown its instance for server {0}",
            "The local server has requested shut down of an Apache Atlas proxy instance.",
            "No action is required.  This is part of the normal operation of the service."),
    EVENT_MAPPER_INITIALIZING("OMRS-ATLAS-REPOSITORY-0006",
            OMRSAuditLogRecordSeverity.INFO,
            "The Apache Atlas event mapper is initializing",
            "The local server has started up a new instance of the Apache Atlas event mapper.",
            "No action is required.  This is part of the normal operation of the service."),
    EVENT_MAPPER_INITIALIZED("OMRS-ATLAS-REPOSITORY-0007",
            OMRSAuditLogRecordSeverity.INFO,
            "The Apache Atlas event mapper has initialized for server {0}",
            "The local server has completed initialization of a new instance.",
            "No action is required.  This is part of the normal operation of the service."),
    EVENT_MAPPER_SHUTDOWN("OMRS-ATLAS-REPOSITORY-0008",
            OMRSAuditLogRecordSeverity.INFO,
            "The Apache Atlas event mapper has shutdown its instance for server {0}",
            "The local server has requested shut down of an Apache Atlas event mapper instance.",
            "No action is required.  This is part of the normal operation of the service."),
    EVENT_MAPPER_STARTING("OMRS-ATLAS-REPOSITORY-0009",
            OMRSAuditLogRecordSeverity.INFO,
            "The Apache Atlas event mapper consumer thread is starting up",
            "The local server has requested startup of an Apache Atlas event mapper consumer.",
            "No action is required.  This is part of the normal operation of the service."),
    EVENT_MAPPER_RUNNING("OMRS-ATLAS-REPOSITORY-0010",
            OMRSAuditLogRecordSeverity.INFO,
            "The Apache Atlas event mapper is running",
            "The local server is now running a consumer thread for Apache Atlas.",
            "No action is required.  This is part of the normal operation of the service."),
    EVENT_MAPPER_CONSUMER_FAILURE("OMRS-ATLAS-REPOSITORY-0011",
            OMRSAuditLogRecordSeverity.EXCEPTION,
            "The Apache Atlas event mapper failed to consume an event",
            "The local server failed to consume an Apache Atlas event.",
            "Investigate the logs for additional information and raise a GitHub issue with the details."),
    ;

    private String logMessageId;
    private OMRSAuditLogRecordSeverity severity;
    private String logMessage;
    private String systemAction;
    private String userAction;


    /**
     * The constructor for OMRSAuditCode expects to be passed one of the enumeration rows defined in
     * OMRSAuditCode above.   For example:
     * <p>
     * OMRSAuditCode   auditCode = OMRSAuditCode.SERVER_NOT_AVAILABLE;
     * <p>
     * This will expand out to the 4 parameters shown below.
     *
     * @param messageId    - unique Id for the message
     * @param severity     - the severity of the message
     * @param message      - text for the message
     * @param systemAction - description of the action taken by the system when the condition happened
     * @param userAction   - instructions for resolving the situation, if any
     */
    ApacheAtlasOMRSAuditCode(String messageId, OMRSAuditLogRecordSeverity severity, String message,
                             String systemAction, String userAction) {
        this.logMessageId = messageId;
        this.severity = severity;
        this.logMessage = message;
        this.systemAction = systemAction;
        this.userAction = userAction;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AuditLogMessageDefinition getMessageDefinition() {
        return new AuditLogMessageDefinition(logMessageId,
                severity,
                logMessage,
                systemAction,
                userAction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AuditLogMessageDefinition getMessageDefinition(String ...params) {
        AuditLogMessageDefinition messageDefinition = new AuditLogMessageDefinition(logMessageId,
                severity,
                logMessage,
                systemAction,
                userAction);
        messageDefinition.setMessageParameters(params);
        return messageDefinition;
    }

}
