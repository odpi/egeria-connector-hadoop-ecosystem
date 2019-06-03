/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.api.TypeDefUpsertRequest;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.instances.EntityInstance;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.api.EntityResponse;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.types.ClassificationTypeDef;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.types.EnumTypeDef;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.types.TypeDefHeader;
import org.odpi.openmetadata.frameworks.connectors.properties.ConnectionProperties;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryConnector;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.OMRSRuntimeException;
import org.springframework.http.*;
import org.springframework.util.Base64Utils;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ApacheAtlasOMRSRepositoryConnector extends OMRSRepositoryConnector {

    private static final Logger log = LoggerFactory.getLogger(ApacheAtlasOMRSRepositoryConnector.class);

    public static final String EP_TYPE_HEADERS = "/api/atlas/v2/types/typedefs/headers";
    public static final String EP_ENTITY = "/api/atlas/v2/entity/guid/";
    public static final String EP_TYPEDEFS = "/api/atlas/v2/types/typedefs";
    public static final String EP_TYPEDEF = "/api/atlas/v2/types/typedef/";

    private String url;
    private String authorization;

    private RestTemplate restTemplate;
    private ObjectMapper objectMapper;

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

        this.restTemplate = new RestTemplate();
        // We must allow single values to be interpreted as arrays for RelationshipAssignments to work properly
        // (without needing to implement a separate bean for every single underlying instance)
        this.objectMapper = new ObjectMapper().enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);

        // Retrieve connection details
        Map<String, Object> proxyProperties = this.connectionBean.getConfigurationProperties();
        this.url = (String) proxyProperties.get("apache.atlas.rest.url");
        String username = (String) proxyProperties.get("apache.atlas.username");
        String password = (String) proxyProperties.get("apache.atlas.password");

        this.authorization = encodeBasicAuth(username, password);

        // Test REST API connection by attempting to retrieve types list
        String types = getTypeDefHeaders();
        successfulInit = types.contains("Referenceable");

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
     * Retrieve the list of TypeDefs that exist in this Apache Atlas environment.
     *
     * @return
     */
    public String getTypeDefHeaders() {
        return makeRequest(EP_TYPE_HEADERS, HttpMethod.GET, null, null);
    }

    /**
     * Retrieve a TypeDef based on its name.
     *
     * @param name the name of the TypeDef to retrieve
     * @return TypeDefHeader
     */
    public TypeDefHeader getTypeDefByName(String name) {

        String endpoint = EP_TYPEDEF + "name/" + name;

        TypeDefHeader typeDefHeader = null;
        ResponseEntity<String> response = null;
        try {
            response = makeUnhandledRequest(endpoint, HttpMethod.GET, null, null);
            typeDefHeader = objectMapper.readValue(response.getBody(), TypeDefHeader.class);
        } catch (RestClientException e) {
            log.warn("Unable to find TypeDef with name: {}", name);
        } catch (IOException e) {
            log.error("Unable to translate TypeDef to an object: {}", response, e);
        }
        return typeDefHeader;

    }

    /**
     * Retrieves an Apache Atlas Entity instance by its GUID, including all of its relationships.
     *
     * @param guid the GUID of the entity instance to retrieve
     * @return EntityInstance
     */
    public EntityInstance getEntityByGUID(String guid) {
        return getEntityByGUID(guid, true);
    }

    /**
     * Retrieve an Apache Atlas Entity instance by its GUID.
     *
     * @param guid the GUID of the entity instance to retrieve
     * @param ignoreRelationships if true, will return only the entity (none of its relationships)
     * @return EntityInstance
     */
    public EntityInstance getEntityByGUID(String guid, boolean ignoreRelationships) {
        String endpoint = EP_ENTITY + guid;
        if (ignoreRelationships) {
            endpoint = endpoint + "?ignoreRelationships=true";
        }
        String entity = makeRequest(endpoint, HttpMethod.GET, null, null);
        EntityResponse entityResponse = null;
        try {
            entityResponse = objectMapper.readValue(entity, EntityResponse.class);
        } catch (IOException e) {
            log.error("Unable to translate entity instance to an object: {}", entity, e);
        }
        return (entityResponse == null ? null : entityResponse.getEntity());
    }

    /**
     * Adds the list of TypeDefs provided to Apache Atlas.
     *
     * @return {@code List<String>} - the list of GUIDs for the TypeDefs that were created
     */
    public TypeDefUpsertRequest createTypeDef(TypeDefHeader typeDef) {

        TypeDefUpsertRequest upsertRequest = new TypeDefUpsertRequest();
        switch (typeDef.getCategory()) {
            case "CLASSIFICATION":
                ArrayList<ClassificationTypeDef> classification = new ArrayList<>();
                classification.add((ClassificationTypeDef)typeDef);
                upsertRequest.setClassificationDefs(classification);
                break;
            case "ENUM":
                ArrayList<EnumTypeDef> enumeration = new ArrayList<>();
                enumeration.add((EnumTypeDef)typeDef);
                upsertRequest.setEnumDefs(enumeration);
                break;
            default:
                log.warn("Requested TypeDef for addition is not (yet) handled -- skipping: {}", typeDef);
                break;
        }

        String payload = null;
        try {
            payload = objectMapper.writeValueAsString(upsertRequest);
        } catch (JsonProcessingException e) {
            log.error("Unable to serialize the request: {}", upsertRequest, e);
        }

        TypeDefUpsertRequest response = null;
        if (payload != null) {
            String result = makeRequest(EP_TYPEDEFS, HttpMethod.POST, MediaType.APPLICATION_JSON, payload);
            try {
                response = objectMapper.readValue(result, TypeDefUpsertRequest.class);
            } catch (IOException e) {
                log.error("Unable to translate creation result to an object: {}", result, e);
            }
        }

        return response;

    }

    /**
     * General utility for making REST API requests.
     *
     * @param endpoint the REST resource against which to make the request
     * @param method HttpMethod (GET, POST, etc)
     * @param contentType the type of content to expect in the payload (if any)
     * @param payload if POSTing some content, the JSON structure providing what should be POSTed
     * @return String - containing the body of the response
     */
    private String makeRequest(String endpoint, HttpMethod method, MediaType contentType, String payload) {

        ResponseEntity<String> response = null;
        try {
            response = makeUnhandledRequest(endpoint, method, contentType, payload);
        } catch (RestClientException e) {
            log.error("Request failed -- check Apache Atlas environment connectivity and authentication details.", e);
        }

        String body = null;
        if (response == null) {
            log.error("Unable to complete request -- check Apache Atlas environment connectivity and authentication details.");
            throw new NullPointerException("Unable to complete request -- check Apache Atlas environment connectivity and authentication details.");
        } else if (response.hasBody()) {
            body = response.getBody();
        }
        return body;

    }

    /**
     * General utility for making REST API requests, throwing any potential errors.
     *
     * @param endpoint the REST resource against which to make the request
     * @param method HttpMethod (GET, POST, etc)
     * @param contentType the type of content to expect in the payload (if any)
     * @param payload if POSTing some content, the JSON structure providing what should be POSTed
     * @return {@code ResponseEntity<String>}
     * @throws RestClientException
     */
    private ResponseEntity<String> makeUnhandledRequest(String endpoint,
                                                        HttpMethod method,
                                                        MediaType contentType,
                                                        String payload) throws RestClientException {

        String url = getBaseURL() + endpoint;

        HttpHeaders headers = getHttpHeaders();
        HttpEntity<String> toSend;
        if (payload != null) {
            headers.setContentType(contentType);
            toSend = new HttpEntity<>(payload, headers);
        } else {
            toSend = new HttpEntity<>(headers);
        }

        if (log.isInfoEnabled()) { log.info("{}ing to {} with: {}", method, url, payload); }

        return restTemplate.exchange(url, method, toSend, String.class);

    }

    /**
     * Setup the HTTP headers of a request (ie. authorization details).
     *
     * @return HttpHeaders
     */
    private HttpHeaders getHttpHeaders() {

        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.CACHE_CONTROL, "no-cache");
        headers.add(HttpHeaders.CONTENT_TYPE, "application/json");

        // Setup authorization header
        String auth = "Basic " + this.authorization;
        headers.add(HttpHeaders.AUTHORIZATION, auth);

        return headers;

    }

    /**
     * Utility function to easily encode a username and password to send through as authorization info.
     *
     * @param username username to encode
     * @param password password to encode
     * @return String of appropriately-encoded credentials for authorization
     */
    private static String encodeBasicAuth(String username, String password) {
        return Base64Utils.encodeToString((username + ":" + password).getBytes(UTF_8));
    }

}
