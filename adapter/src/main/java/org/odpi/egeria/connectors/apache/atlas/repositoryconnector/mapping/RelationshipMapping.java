/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.ApacheAtlasOMRSRepositoryConnector;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores.AttributeTypeDefStore;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores.TypeDefStore;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.*;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.RelationshipDef;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDefAttribute;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper;
import org.odpi.openmetadata.repositoryservices.ffdc.OMRSErrorCode;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.RepositoryErrorException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.TypeErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * The base class for all mappings between OMRS Relationship instances and Apache Atlas relationship instances.
 */
public class RelationshipMapping {

    private static final Logger log = LoggerFactory.getLogger(RelationshipMapping.class);

    private ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector;
    private TypeDefStore typeDefStore;
    private AttributeTypeDefStore attributeDefStore;
    private AtlasRelationship atlasRelationship;
    private String userId;

    /**
     * Mapping itself must be initialized with various objects.
     *
     * @param atlasRepositoryConnector connectivity to an Apache Atlas repository
     * @param typeDefStore the store of mapped TypeDefs for the Atlas repository
     * @param attributeDefStore the store of mapped AttributeTypeDefs for the Atlas repository
     * @param instance the Atlas relationship to be mapped
     * @param userId the user through which to do the mapping
     */
    public RelationshipMapping(ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector,
                               TypeDefStore typeDefStore,
                               AttributeTypeDefStore attributeDefStore,
                               AtlasRelationship.AtlasRelationshipWithExtInfo instance,
                               String userId) {
        this.atlasRepositoryConnector = atlasRepositoryConnector;
        this.typeDefStore = typeDefStore;
        this.attributeDefStore = attributeDefStore;
        this.atlasRelationship = instance.getRelationship();
        this.userId = userId;
    }

    /**
     * Retrieve the mapped OMRS Relationship from the Apache Atlas AtlasRelationship used to construct this mapping object.
     *
     * @return Relationship
     * @throws RepositoryErrorException when unable to retrieve the Relationship
     */
    public Relationship getRelationship() throws RepositoryErrorException {

        final String methodName = "getRelationship";
        OMRSRepositoryHelper omrsRepositoryHelper = atlasRepositoryConnector.getRepositoryHelper();
        String repositoryName = atlasRepositoryConnector.getRepositoryName();
        String atlasRelationshipType = atlasRelationship.getTypeName();
        String omrsRelationshipType = typeDefStore.getMappedOMRSTypeDefName(atlasRelationshipType);

        Relationship omrsRelationship = null;
        if (omrsRelationshipType != null) {

            RelationshipDef typeDef = (RelationshipDef) typeDefStore.getTypeDefByName(omrsRelationshipType);

            // Create the basic skeleton
            omrsRelationship = getSkeletonRelationship(atlasRepositoryConnector, typeDef);

            // Then apply the instance-specific mapping
            omrsRelationship.setGUID(atlasRelationship.getGuid());
            omrsRelationship.setMetadataCollectionId(atlasRepositoryConnector.getMetadataCollectionId());
            switch (atlasRelationship.getStatus()) {
                case ACTIVE:
                    omrsRelationship.setStatus(InstanceStatus.ACTIVE);
                    break;
                case DELETED:
                    omrsRelationship.setStatus(InstanceStatus.DELETED);
                    break;
                default:
                    if (log.isWarnEnabled()) { log.warn("Unhandled relationship status, defaulting to ACTIVE: {}", atlasRelationship.getStatus()); }
                    omrsRelationship.setStatus(InstanceStatus.ACTIVE);
                    break;
            }
            omrsRelationship.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);

            omrsRelationship.setVersion(atlasRelationship.getVersion());
            omrsRelationship.setCreateTime(atlasRelationship.getUpdateTime());
            omrsRelationship.setCreatedBy(atlasRelationship.getCreatedBy());
            omrsRelationship.setUpdatedBy(atlasRelationship.getUpdatedBy());
            omrsRelationship.setUpdateTime(atlasRelationship.getUpdateTime());

            // TODO: assumes the endpoints are always the same ends
            AtlasObjectId atlasEp1 = atlasRelationship.getEnd1();
            AtlasObjectId atlasEp2 = atlasRelationship.getEnd2();

            EntityProxy ep1 = RelationshipMapping.getEntityProxyForObject(
                    atlasRepositoryConnector,
                    typeDefStore,
                    atlasRepositoryConnector.getEntityByGUID(atlasEp1.getGuid(), true, true),
                    userId
            );
            EntityProxy ep2 = RelationshipMapping.getEntityProxyForObject(
                    atlasRepositoryConnector,
                    typeDefStore,
                    atlasRepositoryConnector.getEntityByGUID(atlasEp2.getGuid(), true, true),
                    userId
            );
            if (ep1 != null && ep2 != null) {
                omrsRelationship.setEntityOneProxy(ep1);
                omrsRelationship.setEntityTwoProxy(ep2);
            } else {
                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_RELATIONSHIP_ENDS;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        repositoryName,
                        omrsRelationshipType,
                        atlasRelationshipType,
                        null);
                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                        EntityMapping.class.getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }

            Map<String, Object> atlasRelationshipProperties = atlasRelationship.getAttributes();
            Map<String, TypeDefAttribute> relationshipAttributeMap = typeDefStore.getAllTypeDefAttributesForName(omrsRelationshipType);
            Map<String, String> atlasToOmrsProperties = typeDefStore.getPropertyMappingsForAtlasTypeDef(atlasRelationshipType);
            InstanceProperties omrsRelationshipProperties = new InstanceProperties();
            if (atlasToOmrsProperties != null) {

                for (Map.Entry<String, String> property : atlasToOmrsProperties.entrySet()) {
                    String atlasProperty = property.getKey();
                    String omrsProperty = property.getValue();
                    if (relationshipAttributeMap.containsKey(omrsProperty)) {
                        TypeDefAttribute typeDefAttribute = relationshipAttributeMap.get(omrsProperty);
                        omrsRelationshipProperties = AttributeMapping.addPropertyToInstance(omrsRepositoryHelper,
                                repositoryName,
                                typeDefAttribute,
                                omrsRelationshipProperties,
                                attributeDefStore,
                                atlasRelationshipProperties.get(atlasProperty),
                                methodName);
                    } else {
                        if (log.isWarnEnabled()) { log.warn("No OMRS attribute {} defined for asset type {} -- skipping mapping.", omrsProperty, omrsRelationshipType); }
                    }
                }

            }
            omrsRelationship.setProperties(omrsRelationshipProperties);

        }

        return omrsRelationship;

    }

    /**
     * Retrieves an EntityProxy object for the provided Apache Atlas object.
     *
     * @param atlasRepositoryConnector OMRS connector to the Apache Atlas repository
     * @param typeDefStore store of mapped TypeDefs
     * @param fullAtlasObj the Apache Atlas object for which to retrieve an EntityProxy
     * @param userId the user through which to retrieve the EntityProxy (unused)
     * @return EntityProxy
     */
    static EntityProxy getEntityProxyForObject(ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector,
                                               TypeDefStore typeDefStore,
                                               AtlasEntity.AtlasEntityWithExtInfo fullAtlasObj,
                                               String userId) {

        final String methodName = "getEntityProxyForObject";
        AtlasEntity atlasObj = fullAtlasObj.getEntity();

        EntityProxy entityProxy = null;
        if (atlasObj != null) {

            String repositoryName = atlasRepositoryConnector.getRepositoryName();
            OMRSRepositoryHelper repositoryHelper = atlasRepositoryConnector.getRepositoryHelper();
            String metadataCollectionId = atlasRepositoryConnector.getMetadataCollectionId();

            String atlasTypeName = atlasObj.getTypeName();
            String omrsTypeDefName = typeDefStore.getMappedOMRSTypeDefName(atlasTypeName);

            String qualifiedName;
            Map<String, Object> attributes = atlasObj.getAttributes();
            if (attributes.containsKey("qualifiedName")) {
                qualifiedName = (String) attributes.get("qualifiedName");
            } else {
                log.error("No qualifiedName found for object -- cannot create EntityProxy: {}", atlasObj);
                throw new NullPointerException("No qualifiedName found for object -- cannot create EntityProxy.");
            }

            InstanceProperties uniqueProperties = repositoryHelper.addStringPropertyToInstance(
                    repositoryName,
                    null,
                    "qualifiedName",
                    qualifiedName,
                    methodName
            );

            try {
                entityProxy = repositoryHelper.getNewEntityProxy(
                        repositoryName,
                        metadataCollectionId,
                        InstanceProvenanceType.LOCAL_COHORT,
                        userId,
                        omrsTypeDefName,
                        uniqueProperties,
                        null
                );
                entityProxy.setCreatedBy(atlasObj.getCreatedBy());
                entityProxy.setCreateTime(atlasObj.getCreateTime());
                entityProxy.setUpdatedBy(atlasObj.getUpdatedBy());
                entityProxy.setUpdateTime(atlasObj.getUpdateTime());
                entityProxy.setVersion(atlasObj.getVersion());
            } catch (TypeErrorException e) {
                log.error("Unable to create new EntityProxy.", e);
            }

        } else {
            if (log.isErrorEnabled()) { log.error("No Apache Atlas object provided (was null)."); }
        }

        return entityProxy;

    }

    /**
     * Create the base skeleton of a Relationship, irrespective of the specific Apache Atlas object.
     *
     * @param atlasRepositoryConnector connectivity to an Apache Atlas environment
     * @param omrsRelationshipDef the OMRS RelationshipDef for which to create a skeleton Relationship
     * @return Relationship
     * @throws RepositoryErrorException when unable to create a new skeletal Relationship
     */
    static Relationship getSkeletonRelationship(ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector,
                                                       RelationshipDef omrsRelationshipDef) throws RepositoryErrorException {

        final String methodName = "getSkeletonRelationship";
        Relationship relationship = new Relationship();

        try {
            InstanceType instanceType = atlasRepositoryConnector.getRepositoryHelper().getNewInstanceType(
                    atlasRepositoryConnector.getRepositoryName(),
                    omrsRelationshipDef
            );
            relationship.setType(instanceType);
        } catch (TypeErrorException e) {
            if (log.isErrorEnabled()) { log.error("Unable to construct and set InstanceType -- skipping relationship: {}", omrsRelationshipDef.getName()); }
            OMRSErrorCode errorCode = OMRSErrorCode.INVALID_INSTANCE;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                    omrsRelationshipDef.getName());
            throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                    EntityMapping.class.getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        return relationship;

    }

}
