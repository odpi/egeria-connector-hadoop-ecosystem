/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.mocks;

import org.mockserver.matchers.MatchType;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.JsonBody;

import java.util.List;

import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.JsonBody.json;
import static org.mockserver.model.Parameter.param;

/**
 * A set of constants that can be re-used across various modules' tests.
 */
public class MockConstants {

    public static final String ATLAS_HOST = "localhost";
    public static final String ATLAS_PORT = "1080";
    public static final String ATLAS_ENDPOINT = ATLAS_HOST + ":" + ATLAS_PORT;
    public static final String ATLAS_USER = "admin";
    public static final String ATLAS_PASS = "admin";

    public static final String EGERIA_USER = "admin";
    public static final int EGERIA_PAGESIZE = 100;

    public static final String EXAMPLE_GUID = "4fabaac3-9543-47a5-8c00-c44c81db3cac";
    public static final String EXAMPLE_TYPE_GUID = "aa8d5470-6dbc-4648-9e2f-045e5df9d2f9";
    public static final String EXAMPLE_TYPE_NAME = "RelationalColumn";

    public static final String EXAMPLE_RELATIONSHIP_GUID = "dd44ce51-224d-4c03-a49e-7008292188a6";

    private static final String EP_BASE = "/api/atlas/v2/";
    private static final String EP_TYPES = EP_BASE + "types/";
    private static final String EP_ENTITY = EP_BASE + "entity/";
    private static final String EP_RELATIONSHIP = EP_BASE + "relationship/";
    private static final String EP_SEARCH = EP_BASE + "search/";

    /**
     * Create a mock Atlas response using the provided body.
     * @param body to respond with
     * @return HttpResponse
     */
    public static HttpResponse withResponse(String body) {
        return response()
                .withBody(body)
                .withHeader("Content-Type", "application/json");
    }

    /**
     * Create a mock Atlas typedefs request.
     * @return HttpRequest
     */
    public static HttpRequest typedefsRequest() {
        return request().withMethod("GET").withPath(EP_TYPES + "typedefs/").withHeader("Authorization", "Basic YWRtaW46YWRtaW4=");
    }

    /**
     * Create a mock Atlas typedef-by-name request.
     * @param typeName the name of the type to retrieve
     * @return HttpRequest
     */
    public static HttpRequest typedefRequest(String typeName) {
        return request().withMethod("GET").withPath(EP_TYPES + "typedef/name/" + typeName);
    }

    /**
     * Create a mock Atlas typedef-by-name request for any other typedefs (those not supported).
     * @return HttpRequest
     */
    public static HttpRequest typedefRequest() {
        return request().withMethod("GET").withPath(EP_TYPES + "typedef/name/.*");
    }

    /**
     * Create a mock Atlas typedef upsert request.
     * @param body the body describing the typedef to be upserted
     * @return HttpRequest
     */
    public static HttpRequest typeDefUpsertRequest(JsonBody body) {
        return request().withMethod("POST").withPath(EP_TYPES + "typedefs/").withBody(body);
    }

    /**
     * Create a mock Atlas entity-by-guid request.
     * @param guid the guid of the entity to retrieve
     * @return HttpRequest
     */
    public static HttpRequest entityRequest(String guid) {
        return request().withMethod("GET").withPath(EP_ENTITY + "guid/" + guid);
    }

    /**
     * Create a mock Atlas entity-by-guid, without relationships or any extra info, request.
     * @param guid the guid of the entity to retrieve
     * @return HttpRequest
     */
    public static HttpRequest entityRequestProxy(String guid) {
        return entityRequest(guid).withQueryStringParameters(
                param("ignoreRelationships", "true"),
                param("minExtInfo", "true")
        );
    }

    /**
     * Create a mock Atlas entity-by-guid, without relationships, request.
     * @param guid the guid of the entity to retrieve
     * @return HttpRequest
     */
    public static HttpRequest entityRequestWithoutRelationships(String guid) {
        return entityRequest(guid).withQueryStringParameters(
                param("ignoreRelationships", "true"),
                param("minExtInfo", "false")
        );
    }

    /**
     * Create a mock Atlas entity-by-guid, with relationships, request.
     * @param guid the guid of the entity to retrieve
     * @return HttpRequest
     */
    public static HttpRequest entityRequestWithRelationships(String guid) {
        return entityRequest(guid).withQueryStringParameters(
                param("ignoreRelationships", "false"),
                param("minExtInfo","false")
        );
    }

    /**
     * Create a mock Atlas entity-by-guid request for any other entities (those that should not exist).
     * @return HttpRequest
     */
    public static HttpRequest entityRequest() {
        return request().withMethod("GET").withPath(EP_ENTITY + "guid/.*");
    }

    /**
     * Create a mock Atlas relationship-by-guid request.
     * @param guid the guid of the entity to retrieve
     * @return HttpRequest
     */
    public static HttpRequest relationshipRequest(String guid) {
        return request().withMethod("GET").withPath(EP_RELATIONSHIP + "guid/" + guid).withQueryStringParameters(
                param("extendedInfo", "false")
        );
    }

    /**
     * Create a mock Atlas relationship-by-guid request for any other relationships (those that should not exist).
     * @return HttpRequest
     */
    public static HttpRequest relationshipRequest() {
        return request().withMethod("GET").withPath(EP_RELATIONSHIP + "guid/.*");
    }

    /**
     * Create a mock Atlas basic search request using the provided parameters.
     * @param body the exact-match body of the request
     * @return HttpRequest
     */
    public static HttpRequest basicSearchRequest(String body) {
        return request().withMethod("POST").withPath(EP_SEARCH + "basic").withBody(body);
    }

    /**
     * Create a mock Atlas basic search request using the provided parameters.
     * @param body the JSON body of the request
     * @return HttpRequest
     */
    public static HttpRequest basicSearchRequest(JsonBody body) {
        return request().withMethod("POST").withPath(EP_SEARCH + "basic").withBody(body);
    }

    /**
     * Create a mock Atlas DSL search request using the provided parameters.
     * @param query the query string for the DSL search
     * @return HttpRequest
     */
    public static HttpRequest dslSearchRequest(String query) {
        return request().withMethod("GET").withPath(EP_SEARCH + "dsl").withQueryStringParameters(
                param("query", query)
        );
    }

}
