/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.topic_integrator.atlas.ebi;


import org.odpi.openmetadata.frameworks.connectors.ffdc.ConnectorCheckedException;
import org.odpi.openmetadata.repositoryservices.connectors.openmetadatatopic.OpenMetadataTopicConnector;

public class EbiAtlasConnector extends OpenMetadataTopicConnector
{
   /**
     * Constructor used to initialize the ConnectorProviderBase with the Java class name of the specific
     * store implementation.
     */
    public EbiAtlasConnector()
    {
        super.registerListener(new EbiAtlasTopicListener(auditLog));
    }

    @Override
    public void sendEvent(String event) throws ConnectorCheckedException {
            // nothing to do here
    }
}
