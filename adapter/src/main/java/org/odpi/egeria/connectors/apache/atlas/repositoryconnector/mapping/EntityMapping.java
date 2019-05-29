/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping;

import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.ApacheAtlasOMRSRepositoryConnector;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.EntityInstance;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores.TypeDefStore;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.EntityDetail;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.InstanceProperties;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.InstanceProvenanceType;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.InstanceStatus;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDefAttribute;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.TypeErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Class that generically handles converting an Apache Atlas EntityInstance object into an OMRS EntityDetail object.
 */
public class EntityMapping {

    private static final Logger log = LoggerFactory.getLogger(EntityMapping.class);

    private ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector;
    private TypeDefStore typeDefStore;
    private EntityInstance atlasEntityInstance;
    private String userId;

    public EntityMapping(ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector,
                         TypeDefStore typeDefStore,
                         EntityInstance instance,
                         String userId) {
        this.atlasRepositoryConnector = atlasRepositoryConnector;
        this.typeDefStore = typeDefStore;
        this.atlasEntityInstance = instance;
        this.userId = userId;
    }

    /**
     * Retrieve the mapped OMRS EntityDetail from the Apache Atlas EntityInstance used to construct this mapping object.
     *
     * @return EntityDetail
     */
    public EntityDetail getEntityDetail() {

        final String methodName = "getEntityDetail";
        String atlasTypeDefName = atlasEntityInstance.getTypeName();
        String omrsTypeDefName = typeDefStore.getMappedOMRSTypeDefName(atlasTypeDefName);

        // Create the basic skeleton
        EntityDetail detail = getSkeletonEntityDetail(omrsTypeDefName);

        // Then apply the instance-specific mapping
        if (detail != null) {

            InstanceProperties instanceProperties = new InstanceProperties();
            OMRSRepositoryHelper omrsRepositoryHelper = atlasRepositoryConnector.getRepositoryHelper();
            String repositoryName = atlasRepositoryConnector.getRepositoryName();

            Map<String, TypeDefAttribute> omrsAttributeMap = typeDefStore.getAllTypeDefAttributesForName(omrsTypeDefName);

            // Iterate through the provided mappings to set an OMRS instance property for each one
            Map<String, String> atlasToOmrsProperties = typeDefStore.getPropertyMappingsForAtlasTypeDef(atlasTypeDefName);
            Map<String, String> atlasProperties = atlasEntityInstance.getAttributes();
            Set<String> alreadyMapped = new HashSet<>();
            for (Map.Entry<String, String> property : atlasToOmrsProperties.entrySet()) {
                String atlasProperty = property.getKey();
                String omrsProperty = property.getValue();
                if (omrsAttributeMap.containsKey(omrsProperty)) {
                    TypeDefAttribute typeDefAttribute = omrsAttributeMap.get(omrsProperty);
                    instanceProperties = AttributeMapping.addPrimitivePropertyToInstance(
                            omrsRepositoryHelper,
                            repositoryName,
                            instanceProperties,
                            typeDefAttribute,
                            atlasProperties.get(atlasProperty),
                            methodName
                    );
                    alreadyMapped.add(atlasProperty);
                } else {
                    if (log.isWarnEnabled()) { log.warn("No OMRS attribute {} defined for asset type {} -- skipping mapping.", omrsProperty, omrsTypeDefName); }
                }
            }

            // And map any other simple (non-relationship) properties that are not otherwise mapped into 'additionalProperties'
            Map<String, String> additionalProperties = new HashMap<>();

            Set<String> nonRelationshipSet = atlasProperties.keySet();
            if (nonRelationshipSet != null) {

                // Remove all of the already-mapped properties from our list of non-relationship properties
                nonRelationshipSet.removeAll(alreadyMapped);

                // Iterate through the remaining property names, and add them to a map
                // Note that because 'additionalProperties' is a string-to-string map, we will just convert everything
                // to strings (even arrays of values, we'll concatenate into a single string)
                for (String propertyName : nonRelationshipSet) {
                    String propertyValue = atlasProperties.get(propertyName);
                    if (propertyValue != null) {
                        additionalProperties.put(propertyName, propertyValue);
                    }
                }

                // and finally setup the 'additionalProperties' attribute using this map
                instanceProperties = omrsRepositoryHelper.addStringMapPropertyToInstance(
                        repositoryName,
                        instanceProperties,
                        "additionalProperties",
                        additionalProperties,
                        methodName
                );

            }

            detail.setProperties(instanceProperties);
            // TODO: detail.setReplicatedBy();
            // TODO: detail.setClassifications();
        }

        return detail;

    }

    /**
     * Create the base skeleton of an EntityDetail, irrespective of the specific Apache Atlas object.
     *
     * @param omrsTypeDefName the name of the OMRS TypeDef for which to create a skeleton EntityDetail
     * @return EntityDetail
     */
    private EntityDetail getSkeletonEntityDetail(String omrsTypeDefName) {

        EntityDetail detail = null;
        try {
            detail = atlasRepositoryConnector.getRepositoryHelper().getSkeletonEntity(
                    atlasRepositoryConnector.getRepositoryName(),
                    atlasRepositoryConnector.getMetadataCollectionId(),
                    InstanceProvenanceType.LOCAL_COHORT,
                    userId,
                    omrsTypeDefName
            );
            if (atlasEntityInstance.getStatus().equals("ACTIVE")) {
                detail.setStatus(InstanceStatus.ACTIVE);
            } else {
                log.warn("Unhandled status: {}", atlasEntityInstance.getStatus());
            }
            String guid = atlasEntityInstance.getGuid();
            detail.setGUID(guid);
            detail.setInstanceURL(atlasRepositoryConnector.getBaseURL() + ApacheAtlasOMRSRepositoryConnector.EP_ENTITY + guid);
            detail.setCreatedBy(atlasEntityInstance.getCreatedBy());
            detail.setCreateTime(atlasEntityInstance.getCreateTime());
            detail.setUpdatedBy(atlasEntityInstance.getUpdatedBy());
            detail.setUpdateTime(atlasEntityInstance.getUpdateTime());
            detail.setVersion(atlasEntityInstance.getVersion());
        } catch (TypeErrorException e) {
            log.error("Unable to get skeleton detail entity.", e);
        }

        return detail;

    }

}
