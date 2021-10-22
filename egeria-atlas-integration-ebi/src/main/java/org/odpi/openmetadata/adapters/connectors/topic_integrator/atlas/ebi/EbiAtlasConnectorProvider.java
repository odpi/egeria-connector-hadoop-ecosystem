/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.topic_integrator.atlas.ebi;

import org.odpi.openmetadata.frameworks.connectors.ConnectorProviderBase;
import org.odpi.openmetadata.frameworks.connectors.properties.beans.ConnectorType;

public class EbiAtlasConnectorProvider extends ConnectorProviderBase
{
    static final String  connectorTypeGUID = "461f9fae-326d-11ec-8d3d-0242ac130003";
    static final String  connectorTypeName = "EBI Atlas connector";
    static final String  connectorTypeDescription = "EBI Atlas connector";

   /**
     * Constructor used to initialize the ConnectorProviderBase with the Java class name of the specific
     * store implementation.
     */
    public EbiAtlasConnectorProvider()
    {
        Class<?> connectorClass = EbiAtlasConnector.class;

        super.setConnectorClassName(connectorClass.getName());


        ConnectorType connectorType = new ConnectorType();
        connectorType.setType(ConnectorType.getConnectorTypeType());
        connectorType.setGUID(connectorTypeGUID);
        connectorType.setQualifiedName(connectorTypeName);
        connectorType.setDisplayName(connectorTypeName);
        connectorType.setDescription(connectorTypeDescription);
        connectorType.setConnectorProviderClassName(this.getClass().getName());

        super.connectorTypeBean = connectorType;
    }
}
