/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.mocks;

import org.mockserver.client.MockServerClient;
import org.mockserver.client.initialize.ExpectationInitializer;
import org.mockserver.matchers.MatchType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

import static org.mockserver.model.JsonBody.json;

import static org.odpi.egeria.connectors.apache.atlas.mocks.MockConstants.*;

/**
 * Setup a mock server to act as an Apache Atlas REST API endpoint against which we can do some thorough testing.
 */
public class MockServerExpectations implements ExpectationInitializer {

    private static final Logger log = LoggerFactory.getLogger(MockServerExpectations.class);

    /**
     * Setup the expectations we will need to respond to various tests.
     * @param mockServerClient the client against which to set the expectations
     */
    @Override
    public void initializeExpectations(MockServerClient mockServerClient) {

        initializeTypeDetails(mockServerClient);
        initializeKnownEntities(mockServerClient);
        setSearchByProperty(mockServerClient);
        setSearchByPropertyValue(mockServerClient);

        // Finally, set any others to default to not finding any results (should always be last)
        setNotFoundDefaults(mockServerClient);

    }

    private void initializeTypeDetails(MockServerClient mockServerClient) {

        setTypesQuery(mockServerClient);
        Resource[] typeFiles = getFilesMatchingPattern("types/*.json");
        if (typeFiles != null) {
            for (Resource typeFile : typeFiles) {
                setTypeDetails(mockServerClient, typeFile.getFilename());
            }
        }

    }

    private void initializeKnownEntities(MockServerClient mockServerClient) {

        Resource[] instanceExamples = getFilesMatchingPattern("entity_by_guid/*.json");
        if (instanceExamples != null) {
            for (Resource instanceExample : instanceExamples) {
                setDetailsByGuid(mockServerClient, instanceExample);
            }
        }

    }

    private void setDetailsByGuid(MockServerClient mockServerClient, Resource resource) {
        URL url = null;
        try {
            url = resource.getURL();
        } catch (IOException e) {
            log.error("Unable to retrieve detailed file from: {}", resource, e);
        }
        if (url != null) {
            String filename = url.getFile();
            String guid = getGuidFromFilename(filename);
            mockServerClient
                    .when(entityRequestWithoutRelationships(guid))
                    .respond(withResponse(getResourceFileContents("entity_by_guid" + File.separator + guid + ".json")));
        }
    }

    private String getGuidFromFilename(String filename) {
        return filename.substring(filename.lastIndexOf("/") + 1, filename.indexOf(".json"));
    }

    private void setNotFoundDefaults(MockServerClient mockServerClient) {
        setDefaultNoTypeFound(mockServerClient);
        setDefaultNoEntityFound(mockServerClient);
        setDefaultNoRelationshipFound(mockServerClient);
    }

    private void setTypesQuery(MockServerClient mockServerClient) {
        mockServerClient
                .when(typedefsRequest())
                .respond(withResponse(getResourceFileContents("types.json")));
    }

    private void setTypeDetails(MockServerClient mockServerClient, String typeFilename) {
        String typeName = typeFilename.substring(0, typeFilename.indexOf(".json"));
        mockServerClient
                .when(typedefRequest(typeName))
                .respond(withResponse(getResourceFileContents("types" + File.separator + typeName + ".json")));
    }

    private void setSearchByProperty(MockServerClient mockServerClient) {
        String caseName = "SearchByProperty";
        mockServerClient.when(basicSearchRequest(
                json(
                        "{\"typeName\":\"hbase_column_family\",\"excludeDeletedEntities\":false,\"includeClassificationAttributes\":true,\"entityFilters\":{\"attributeName\":\"dataBlockEncoding\",\"operator\":\"=\",\"attributeValue\":\"FAST_DIFF\"}}",
                        MatchType.ONLY_MATCHING_FIELDS
                )))
                .respond(withResponse(getResourceFileContents("by_case" + File.separator + caseName + File.separator + "results_FAST_DIFF.json")));
        mockServerClient.when(basicSearchRequest(
                json(
                        "{\"typeName\":\"hbase_column_family\",\"excludeDeletedEntities\":false,\"includeClassificationAttributes\":true,\"entityFilters\":{\"attributeName\":\"dataBlockEncoding\",\"operator\":\"!=\",\"attributeValue\":\"FAST_DIFF\"}}",
                        MatchType.ONLY_MATCHING_FIELDS
                )))
                .respond(withResponse(getResourceFileContents("by_case" + File.separator + caseName + File.separator + "results_NONE.json")));
        mockServerClient.when(basicSearchRequest(
                json(
                        "{\"typeName\":\"hbase_column_family\",\"excludeDeletedEntities\":false,\"includeClassificationAttributes\":true,\"entityFilters\":{\"condition\":\"OR\", \"criterion\":[{\"attributeName\":\"dataBlockEncoding\",\"operator\":\"=\",\"attributeValue\":\"FAST_DIFF\"},{\"attributeName\":\"name\",\"operator\":\"=\",\"attributeValue\":\"t\"}]}}",
                        MatchType.ONLY_MATCHING_FIELDS
                )))
                .respond(withResponse(getResourceFileContents("by_case" + File.separator + caseName + File.separator + "results_ANY.json")));
        mockServerClient
                .when(basicSearchRequest(
                        json(
                                "{\"typeName\":\"hbase_column_family\",\"excludeDeletedEntities\":false,\"includeClassificationAttributes\":true}",
                                MatchType.ONLY_MATCHING_FIELDS
                        )))
                .respond(withResponse(getResourceFileContents("by_case" + File.separator + caseName + File.separator + "results_all.json")));
    }

    private void setSearchByPropertyValue(MockServerClient mockServerClient) {
        String caseName = "SearchByPropertyValue";
        mockServerClient.when(basicSearchRequest(
                json(
                        "{\"typeName\":\"hbase_column_family\",\"excludeDeletedEntities\":false,\"includeClassificationAttributes\":true,\"entityFilters\":{\"condition\":\"OR\",\"criterion\":[{\"attributeName\":\"owner\",\"operator\":\"=\",\"attributeValue\":\"atlas\"},{\"attributeName\":\"name\",\"operator\":\"=\",\"attributeValue\":\"atlas\"},{\"attributeName\":\"qualifiedName\",\"operator\":\"=\",\"attributeValue\":\"atlas\"},{\"attributeName\":\"dataBlockEncoding\",\"operator\":\"=\",\"attributeValue\":\"atlas\"}]}}",
                        MatchType.ONLY_MATCHING_FIELDS
                )))
                .respond(withResponse(getResourceFileContents("by_case" + File.separator + caseName + File.separator + "results_exact.json")));
        mockServerClient.when(basicSearchRequest(
                json(
                        "{\"typeName\":\"hbase_column_family\",\"excludeDeletedEntities\":false,\"includeClassificationAttributes\":true,\"entityFilters\":{\"condition\":\"OR\",\"criterion\":[{\"attributeName\":\"owner\",\"operator\":\"startsWith\",\"attributeValue\":\"atl\"},{\"attributeName\":\"name\",\"operator\":\"startsWith\",\"attributeValue\":\"atl\"},{\"attributeName\":\"qualifiedName\",\"operator\":\"startsWith\",\"attributeValue\":\"atl\"},{\"attributeName\":\"dataBlockEncoding\",\"operator\":\"startsWith\",\"attributeValue\":\"atl\"}]}}",
                        MatchType.ONLY_MATCHING_FIELDS
                )))
                .respond(withResponse(getResourceFileContents("by_case" + File.separator + caseName + File.separator + "results_startsWith.json")));
        mockServerClient.when(basicSearchRequest(
                json(
                        "{\"typeName\":\"hbase_column_family\",\"excludeDeletedEntities\":false,\"includeClassificationAttributes\":true,\"entityFilters\":{\"condition\":\"OR\",\"criterion\":[{\"attributeName\":\"owner\",\"operator\":\"contains\",\"attributeValue\":\"tla\"},{\"attributeName\":\"name\",\"operator\":\"contains\",\"attributeValue\":\"tla\"},{\"attributeName\":\"qualifiedName\",\"operator\":\"contains\",\"attributeValue\":\"tla\"},{\"attributeName\":\"dataBlockEncoding\",\"operator\":\"contains\",\"attributeValue\":\"tla\"}]}}",
                        MatchType.ONLY_MATCHING_FIELDS
                )))
                .respond(withResponse(getResourceFileContents("by_case" + File.separator + caseName + File.separator + "results_contains.json")));
        mockServerClient.when(basicSearchRequest(
                json(
                        "{\"typeName\":\"hbase_column_family\",\"excludeDeletedEntities\":false,\"includeClassificationAttributes\":true,\"entityFilters\":{\"condition\":\"OR\",\"criterion\":[{\"attributeName\":\"owner\",\"operator\":\"endsWith\",\"attributeValue\":\"las\"},{\"attributeName\":\"name\",\"operator\":\"endsWith\",\"attributeValue\":\"las\"},{\"attributeName\":\"qualifiedName\",\"operator\":\"endsWith\",\"attributeValue\":\"las\"},{\"attributeName\":\"dataBlockEncoding\",\"operator\":\"endsWith\",\"attributeValue\":\"las\"}]}}",
                        MatchType.ONLY_MATCHING_FIELDS
                )))
                .respond(withResponse(getResourceFileContents("by_case" + File.separator + caseName + File.separator + "results_endsWith.json")));
    }

    private void setDefaultNoTypeFound(MockServerClient mockServerClient) {
        mockServerClient
                .when(typedefRequest())
                .respond(withResponse("{\"errorCode\":\"ATLAS-404-00-001\",\"errorMessage\":\"Given typename PLACEHOLDER was invalid\"}").withStatusCode(404));
    }

    private void setDefaultNoEntityFound(MockServerClient mockServerClient) {
        mockServerClient
                .when(entityRequest())
                .respond(withResponse("{\"errorCode\":\"ATLAS-404-00-005\",\"errorMessage\":\"Given instance guid PLACEHOLDER is invalid/not found\"}").withStatusCode(404));
    }

    private void setDefaultNoRelationshipFound(MockServerClient mockServerClient) {
        mockServerClient
                .when(relationshipRequest())
                .respond(withResponse("{\"errorCode\":\"ATLAS-404-00-00C\",\"errorMessage\":\"Given relationship guid 123 is invalid/not found\"}").withStatusCode(404));
    }

    /**
     * Retrieve the contents of a test resource file.
     * @return String
     */
    private String getResourceFileContents(String filename) {

        ClassPathResource resource = new ClassPathResource(filename);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            log.error("Unable to read resource file: {}", filename, e);
        }
        if (reader != null) {
            return reader.lines().collect(Collectors.joining(System.lineSeparator()));
        }
        return null;

    }

    /**
     * Retrieve the set of resources that match the specified pattern.
     * @param pattern to match for retrieving resources
     * @return {@code Resource[]}
     */
    private Resource[] getFilesMatchingPattern(String pattern) {

        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        try {
            return resolver.getResources(pattern);
        } catch(IOException e) {
            log.error("Unable to find any matches to pattern: {}", pattern, e);
        }
        return null;

    }

}
