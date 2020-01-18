/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.auditlog;

import java.text.MessageFormat;

public enum ApacheAtlasOMRSErrorCode {

    REST_CLIENT_FAILURE(500, "OMRS-ATLAS-REPOSITORY-500-001 ",
            "The Apache Atlas REST API was not successfully initialized to \"{0}\"",
            "The system was unable to login to or access the Apache Atlas environment via REST API.",
            "Check your authorization details are accurate, the Apache Atlas environment started, and is network-accessible."),
    INVALID_SEARCH(500, "OMRS-ATLAS-REPOSITORY-500-002 ",
            "The Apache Atlas system was unable to process the search \"{0}\"",
            "The system was unable to run the search against Apache Atlas via REST API.",
            "Check the system logs and diagnose or report the problem."),
    REGEX_NOT_IMPLEMENTED(501, "OMRS-ATLAS-REPOSITORY-501-001 ",
            "Repository {0} is not able to support the regular expression \"{1}\"",
            "This repository has a fixed subset of regular expressions it can support.",
            "No action required, this is a limitation of the technology. To search using such regular expressions, the metadata of interest" +
                    " must be synchronized to a cohort repository that can support such regular expressions."),
    NO_HISTORY(501, "OMRS-ATLAS-REPOSITORY-501-002 ",
            "Repository {0} is not able to service historical queries",
            "This repository does not retain historical metadata, so cannot support historical queries.",
            "No action required, this is a limitation of the technology. To search such history, the metadata of interest" +
                    " must be synchronized to a cohort repository that can support history."),
    EVENT_MAPPER_NOT_INITIALIZED(400, "OMRS-ATLAS-REPOSITORY-400-001 ",
            "There is no valid event mapper for repository \"{1}\"",
            "Appropriate event could not be produced for request",
            "Check the system logs and diagnose or report the problem."),
    EVENT_MAPPER_IMPROPERLY_INITIALIZED(400, "OMRS-ATLAS-REPOSITORY-400-002 ",
            "The event mapper has been improperly initialized for repository \"{1}\"",
            "The system will be unable to process any events",
            "Check the system logs and diagnose or report the problem."),
    TYPEDEF_NOT_SUPPORTED(404, "OMRS-ATLAS-REPOSITORY-404-001 ",
            "The typedef \"{0}\" is not supported by repository \"{1}\"",
            "The system is currently unable to support the requested the typedef.",
            "Request support through Egeria GitHub issue."),
    ENTITY_NOT_KNOWN(404, "OMRS-ATLAS-REPOSITORY-404-002 ",
            "The entity identified with guid {0} passed on the {1} call is not known to the open metadata repository {2}",
            "The system is unable to retrieve the properties for the requested entity because the supplied guid is not recognized.",
            "The guid is supplied by the caller to the server.  It may have a logic problem that has corrupted the guid, or the entity has been deleted since the guid was retrieved."),
    RELATIONSHIP_NOT_KNOWN(404, "OMRS-ATLAS-REPOSITORY-404-003 ",
            "The relationship identified with guid {0} passed on the {1} call is not known to the open metadata repository {2}",
            "The system is unable to retrieve the properties for the requested relationship because the supplied guid is not recognized.",
            "The guid is supplied by the caller to the OMRS.  It may have a logic problem that has corrupted the guid, or the relationship has been deleted since the guid was retrieved."),

    ;

    private int    httpErrorCode;
    private String errorMessageId;
    private String errorMessage;
    private String systemAction;
    private String userAction;

    /**
     * The constructor for LocalAtlasOMRSErrorCode expects to be passed one of the enumeration rows defined in
     * LocalAtlasOMRSErrorCode above.   For example:
     *
     *     LocalAtlasOMRSErrorCode   errorCode = LocalAtlasOMRSErrorCode.NULL_INSTANCE;
     *
     * This will expand out to the 5 parameters shown below.
     *
     * @param newHTTPErrorCode - error code to use over REST calls
     * @param newErrorMessageId - unique Id for the message
     * @param newErrorMessage - text for the message
     * @param newSystemAction - description of the action taken by the system when the error condition happened
     * @param newUserAction - instructions for resolving the error
     */
    ApacheAtlasOMRSErrorCode(int newHTTPErrorCode, String newErrorMessageId, String newErrorMessage, String newSystemAction, String newUserAction) {
        this.httpErrorCode  = newHTTPErrorCode;
        this.errorMessageId = newErrorMessageId;
        this.errorMessage   = newErrorMessage;
        this.systemAction   = newSystemAction;
        this.userAction     = newUserAction;
    }


    public int getHTTPErrorCode() {
        return httpErrorCode;
    }


    /**
     * Returns the unique identifier for the error message.
     *
     * @return errorMessageId
     */
    public String getErrorMessageId() {
        return errorMessageId;
    }


    /**
     * Returns the error message with placeholders for specific details.
     *
     * @return errorMessage (unformatted)
     */
    public String getUnformattedErrorMessage() {
        return errorMessage;
    }


    /**
     * Returns the error message with the placeholders filled out with the supplied parameters.
     *
     * @param params - strings that plug into the placeholders in the errorMessage
     * @return errorMessage (formatted with supplied parameters)
     */
    public String getFormattedErrorMessage(String... params) {
        MessageFormat mf = new MessageFormat(errorMessage);
        return mf.format(params);
    }


    /**
     * Returns a description of the action taken by the system when the condition that caused this exception was
     * detected.
     *
     * @return systemAction
     */
    public String getSystemAction() {
        return systemAction;
    }


    /**
     * Returns instructions of how to resolve the issue reported in this exception.
     *
     * @return userAction
     */
    public String getUserAction() {
        return userAction;
    }

}
