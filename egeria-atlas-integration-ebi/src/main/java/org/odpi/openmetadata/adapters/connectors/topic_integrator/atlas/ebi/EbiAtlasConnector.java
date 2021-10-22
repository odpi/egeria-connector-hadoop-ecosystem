/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.openmetadata.adapters.connectors.topic_integrator.atlas.ebi;

import org.odpi.openmetadata.frameworks.connectors.ffdc.ConnectorCheckedException;
//import org.odpi.openmetadata.repositoryservices.connectors.openmetadatatopic.OpenMetadataTopicConnector;
import org.odpi.openmetadata.frameworks.connectors.properties.ConnectionProperties;
import org.odpi.openmetadata.frameworks.connectors.properties.EndpointProperties;
import org.odpi.openmetadata.integrationservices.topic.connector.TopicIntegratorConnector;
import  org.odpi.openmetadata.integrationservices.topic.connector.TopicIntegratorContext;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class EbiAtlasConnector extends TopicIntegratorConnector
{
    private TopicIntegratorContext myContext = null;

   /**
     * Constructor used to initialize the ConnectorProviderBase with the Java class name of the specific
     * store implementation.
     */
//    public EbiAtlasConnector()
//    {
//        super.registerListener(new EbiAtlasTopicListener(null));
//    }
//        private String templateQualifiedName = null;
//        private String templateGUID = null;
//        private String targetRootURL = null;




        /**
         * Initialize the connector.
         *
         * @param connectorInstanceId - unique id for the connector instance - useful for messages etc
         * @param connectionProperties - POJO for the configuration used to create the connector.
         */
        @Override
        public void initialize(String connectorInstanceId, ConnectionProperties connectionProperties)
        {
            super.initialize(connectorInstanceId, connectionProperties);
//            registerListener(new EbiAtlasTopicListener(auditLog));

            EndpointProperties endpoint = connectionProperties.getEndpoint();

//            if (endpoint != null)
//            {
//                targetRootURL = endpoint.getAddress();
//            }

            Map<String, Object> configurationProperties = connectionProperties.getConfigurationProperties();

//            if (configurationProperties != null)
//            {
//                templateQualifiedName = configurationProperties.get(KafkaMonitorIntegrationProvider.TEMPLATE_QUALIFIED_NAME_CONFIGURATION_PROPERTY).toString();
//            }
        }


        /**
         * Indicates that the connector is completely configured and can begin processing.
         * This call can be used to register with non-blocking services.
         *
         * @throws ConnectorCheckedException there is a problem within the connector.
         */
        @Override
        public void start() throws ConnectorCheckedException
        {
            super.start();

            final String methodName = "start";

            myContext = super.getContext();

            /*
             * Record the configuration
             */
//            if (auditLog != null)
//            {
//                auditLog.logMessage(methodName,
//                                    KafkaIntegrationConnectorAuditCode.CONNECTOR_CONFIGURATION.getMessageDefinition(connectorName,
//                                                                                                                    targetRootURL,
//                                                                                                                    templateQualifiedName));
//            }

            /*
             * Retrieve the template if one has been requested
             */
//            if (templateQualifiedName != null)
//            {
//                try
//                {
//                    List<TopicElement> templateElements = myContext.getTopicsByName(templateQualifiedName, 0, 0);
//
//                    if (templateElements != null)
//                    {
//                        for (TopicElement templateElement : templateElements)
//                        {
//                            String qualifiedName = templateElement.getProperties().getQualifiedName();
//
//                            if (templateQualifiedName.equals(qualifiedName))
//                            {
//                                templateGUID = templateElement.getElementHeader().getGUID();
//                            }
//                        }
//                    }
//                }
//                catch (Exception error)
//                {
//                    if (auditLog != null)
//                    {
//                        auditLog.logException(methodName,
//                                              KafkaIntegrationConnectorAuditCode.MISSING_TEMPLATE.getMessageDefinition(connectorName, templateQualifiedName),
//                                              error);
//                    }
//
//                }
//            }
        }
    /**
     * Requests that the connector does a comparison of the metadata in the third party technology and open metadata repositories.
     * Refresh is called when the integration connector first starts and then at intervals defined in the connector's configuration
     * as well as any external REST API calls to explicitly refresh the connector.
     *
     * This method performs two sweeps.  It first retrieves the files in the directory and validates that are in the
     * catalog - adding or updating them if necessary.  The second sweep is to ensure that all of the assets catalogued
     * in this directory actually exist on the file system.
     *
     * @throws ConnectorCheckedException there is a problem with the connector.  It is not able to refresh the metadata.
     */
    @Override
    public void refresh() throws ConnectorCheckedException {
        // no supported
    }

        /**
         * Shutdown file monitoring
         *
         * @throws ConnectorCheckedException something failed in the super class
         */
        @Override
        public void disconnect() throws ConnectorCheckedException
        {
            final String methodName = "disconnect";


//            if (auditLog != null)
//            {
//                auditLog.logMessage(methodName,
//                                    KafkaIntegrationConnectorAuditCode.CONNECTOR_STOPPING.getMessageDefinition(connectorName));
//            }

            super.disconnect();
        }

}
