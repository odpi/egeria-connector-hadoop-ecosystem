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
        return request().withMethod("GET").withPath(EP_TYPES + "typedefs/");
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
     * Create a mock Atlas entity-by-guid request.
     * @param guid the guid of the entity to retrieve
     * @return HttpRequest
     */
    public static HttpRequest entityRequest(String guid) {
        return request().withMethod("GET").withPath(EP_ENTITY + "guid/" + guid);
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
        return request().withMethod("GET").withPath(EP_RELATIONSHIP + "guid/" + guid);
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

}
