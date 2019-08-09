/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector;

import java.text.MessageFormat;

public enum ApacheAtlasOMRSErrorCode {

    REST_CLIENT_FAILURE(500, "OMRS-ATLAS-REPOSITORY-500-001 ",
            "The Apache Atlas REST API was not successfully initialized to \"{0}\"",
            "The system was unable to login to or access the Apache Atlas environment via REST API.",
            "Check your authorization details are accurate, the Apache Atlas environment started, and is network-accessible."),
    INSTANCE_ALREADY_HOME(500, "OMRS-ATLAS-REPOSITORY-500-002 ",
            "The instance provided to the method \"{0}\" is owned by repository \"{1}\" so cannot be treated as a reference copy",
            "The system was unable to process the instance provided, because it cannot be treated as a reference copy in its home repository.",
            "Check your calling method is using the appropriate methods for home vs reference copies of instances."),
    INVALID_INSTANCE_HEADER(500, "OMRS-ATLAS-REPOSITORY-500-003 ",
            "The instance supplied to method \"{0}\" on repository \"{1}\" is missing header fields \"{2}\"",
            "The instance is missing essential header fields",
            "Please verify the format and content of the event describing the instance."),
    UNABLE_TO_SAVE(500, "OMRS-ATLAS-REPOSITORY-500-004 ",
            "The instance supplied to method \"{0}\" on repository \"{1}\" could not be persisted",
            "The system was unable to persist the instance provided.",
            "Check the logs of the underlying Apache Atlas repository for reasons why persistence may have failed."),
    CONFLICTING_GUID_FOR_REFERENCE(500, "OMRS-ATLAS-REPOSITORY-500-005 ",
            "The instance supplied to method \"{0}\" on repository \"{1}\" with guid \"{2}\" is already present, but not as a reference",
            "The system cannot persist this instance as a reference when it already exists as a non-reference.",
            "Check for the source of the conflict for this instance's GUID."),
    REGEX_NOT_IMPLEMENTED(501, "OMRS-ATLAS-REPOSITORY-501-001 ",
            "Repository {0} is not able to support the regular expression \"{1}\"",
            "This repository has a fixed subset of regular expressions it can support.",
            "No action required, this is a limitation of the technology. To search using such regular expressions, the metadata of interest" +
                    " must be synchronized to a cohort repository that can support such regular expressions."),
    INSTANCE_NOT_PROVIDED(404, "OMRS-ATLAS-REPOSITORY-404-001 ",
            "The instance provided to the method \"{0}\" of repository \"{1}\" was null",
            "The system was unable to process the instance provided, because no instance was provided.",
            "Check your calling method is actually providing a valid instance."),
    TYPEDEF_NOT_KNOWN_FOR_INSTANCE(404, "OMRS-ATLAS-REPOSITORY-404-002 ",
            "The typedef \"{0}\" found on the instance with guid \"{1}\" passed to method \"{2}\" is not supported by repository \"{3}\"",
            "The system was unable to process the instance provided, because it does not support the instance's type.",
            "Check your calling method is validating type support before sending instances."),
    PROPERTY_NOT_KNOWN_FOR_INSTANCE(404, "OMRS-ATLAS-REPOSITORY-404-003 ",
            "The property \"{0}\" is not mapped for typedef \"{1}\" in repository \"{2}\"",
            "The system was unable to set the property provided on the instance, because it does not support the property.",
            "Consider extending the properties of the repository to avoid this error in the future."),
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
