/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.mocks;

import org.odpi.openmetadata.frameworks.connectors.properties.beans.Connection;
import org.odpi.openmetadata.frameworks.connectors.properties.beans.ConnectorType;
import org.odpi.openmetadata.frameworks.connectors.properties.beans.Endpoint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mocked connection for the ApacheAtlasOMRSRepositoryConnector.
 */
public class MockConnection extends Connection {

    public MockConnection() {

        super();

        setDisplayName("Mock Apache Atlas Connection");
        setDescription("A pretend Apache Atlas connection.");

        ConnectorType connectorType = new ConnectorType();
        connectorType.setConnectorProviderClassName("org.odpi.egeria.connectors.apache.atlas.repositoryconnector.ApacheAtlasOMRSRepositoryConnectorProvider");
        setConnectorType(connectorType);

        Endpoint endpoint = new Endpoint();
        endpoint.setAddress(MockConstants.ATLAS_ENDPOINT);
        endpoint.setProtocol("http");
        setEndpoint(endpoint);

        setUserId(MockConstants.ATLAS_USER);
        setClearPassword(MockConstants.ATLAS_PASS);

        Map<String, Object> configProperties = new HashMap<>();
        List<String> defaultZones = new ArrayList<>();
        defaultZones.add("default");
        configProperties.put("defaultZones", defaultZones);
        setConfigurationProperties(configProperties);

    }

}
