/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector;

import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.SearchFilter;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.odpi.openmetadata.frameworks.connectors.properties.ConnectionProperties;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryConnector;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.OMRSRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.atlas.AtlasClientV2;

import java.util.Map;

public class ApacheAtlasOMRSRepositoryConnector extends OMRSRepositoryConnector {

    private static final Logger log = LoggerFactory.getLogger(ApacheAtlasOMRSRepositoryConnector.class);

    public static final String EP_ENTITY = "/api/atlas/v2/entity/guid/";

    private String url;
    private AtlasClientV2 atlasClient;
    private boolean successfulInit = false;

    /**
     * Default constructor used by the OCF Connector Provider.
     */
    public ApacheAtlasOMRSRepositoryConnector() {
        // Nothing to do...
    }

    /**
     * Call made by the ConnectorProvider to initialize the Connector with the base services.
     *
     * @param connectorInstanceId   unique id for the connector instance   useful for messages etc
     * @param connectionProperties   POJO for the configuration used to create the connector.
     */
    @Override
    public void initialize(String               connectorInstanceId,
                           ConnectionProperties connectionProperties) {
        super.initialize(connectorInstanceId, connectionProperties);

        final String methodName = "initialize";
        if (log.isDebugEnabled()) { log.debug("Initializing ApacheAtlasOMRSRepositoryConnector..."); }

        // Retrieve connection details
        Map<String, Object> proxyProperties = this.connectionBean.getConfigurationProperties();
        this.url = (String) proxyProperties.get("apache.atlas.rest.url");
        String username = (String) proxyProperties.get("apache.atlas.username");
        String password = (String) proxyProperties.get("apache.atlas.password");

        this.atlasClient = new AtlasClientV2(new String[]{ getBaseURL() }, new String[]{ username, password });

        // Test REST API connection by attempting to retrieve types list
        try {
            AtlasTypesDef atlasTypes = atlasClient.getAllTypeDefs(new SearchFilter());
            successfulInit = (atlasTypes != null && atlasTypes.hasEntityDef("Referenceable"));
        } catch (AtlasServiceException e) {
            log.error("Unable to retrieve types from Apache Atlas.", e);
        }

        if (!successfulInit) {
            ApacheAtlasOMRSErrorCode errorCode = ApacheAtlasOMRSErrorCode.REST_CLIENT_FAILURE;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(this.url);
            throw new OMRSRuntimeException(
                    errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction()
            );
        }

    }

    /**
     * Set up the unique Id for this metadata collection.
     *
     * @param metadataCollectionId - String unique Id
     */
    @Override
    public void setMetadataCollectionId(String metadataCollectionId) {
        this.metadataCollectionId = metadataCollectionId;
        /*
         * Initialize the metadata collection only once the connector is properly set up.
         * (Meaning we will NOT initialise the collection if the connector failed to setup properly.)
         */
        if (successfulInit) {
            metadataCollection = new ApacheAtlasOMRSMetadataCollection(this,
                    serverName,
                    repositoryHelper,
                    repositoryValidator,
                    metadataCollectionId);
        }
    }

    /**
     * Retrieve the base URL of the Apache Atlas environment.
     *
     * @return String
     */
    public String getBaseURL() {
        return this.url;
    }

    /**
     * Indicates whether the provided TypeDef exists in this Apache Atlas environment.
     *
     * @param name the name of the TypeDef in Apache Atlas to check
     * @return boolean
     */
    public boolean typeDefExistsByName(String name) {
        return atlasClient.typeWithNameExists(name);
    }

    /**
     * Retrieves an Apache Atlas Entity instance by its GUID, including all of its relationships.
     *
     * @param guid the GUID of the entity instance to retrieve
     * @return AtlasEntityWithExtInfo
     */
    public AtlasEntity.AtlasEntityWithExtInfo getEntityByGUID(String guid) {
        return getEntityByGUID(guid, false, true);
    }

    /**
     * Retrieve an Apache Atlas Entity instance by its GUID.
     *
     * @param guid the GUID of the entity instance to retrieve
     * @param minimalExtraInfo if true, minimize the amount of extra information retrieved about the GUID
     * @param ignoreRelationships if true, will return only the entity (none of its relationships)
     * @return AtlasEntityWithExtInfo
     */
    public AtlasEntity.AtlasEntityWithExtInfo getEntityByGUID(String guid, boolean minimalExtraInfo, boolean ignoreRelationships) {
        AtlasEntity.AtlasEntityWithExtInfo entity = null;
        try {
            entity = atlasClient.getEntityByGuid(guid, minimalExtraInfo, ignoreRelationships);
        } catch (AtlasServiceException e) {
            log.error("Unable to retrieve entity by GUID: {}", guid, e);
        }
        return entity;
    }

    /**
     * Adds the list of TypeDefs provided to Apache Atlas.
     *
     * @param typeDefs the TypeDefs to add to Apache Atlas
     * @return AtlasTypesDef
     */
    public AtlasTypesDef createTypeDef(AtlasTypesDef typeDefs) {
        AtlasTypesDef result = null;
        try {
            result = atlasClient.createAtlasTypeDefs(typeDefs);
        } catch (AtlasServiceException e) {
            log.error("Unable to create provided TypeDefs: {}", typeDefs, e);
        }
        return result;
    }

}
