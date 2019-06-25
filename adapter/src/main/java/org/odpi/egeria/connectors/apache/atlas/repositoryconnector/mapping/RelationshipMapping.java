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

import java.util.Date;
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

        /*final String methodName = "getRelationship";
        OMRSRepositoryHelper omrsRepositoryHelper = atlasRepositoryConnector.getRepositoryHelper();
        String repositoryName = atlasRepositoryConnector.getRepositoryName();*/
        String atlasRelationshipType = atlasRelationship.getTypeName();
        // TODO: currently all mappings from Atlas RelationshipDef to OMRS RelationshipDef are one-to-n, so never a prefix
        String omrsRelationshipType = typeDefStore.getMappedOMRSTypeDefName(atlasRelationshipType, null);

        Relationship omrsRelationship = null;
        if (omrsRelationshipType != null) {

            // TODO: assumes the endpoints are always the same ends
            AtlasObjectId atlasEp1 = atlasRelationship.getEnd1();
            AtlasObjectId atlasEp2 = atlasRelationship.getEnd2();

            // TODO: currently all mappings from Atlas RelationshipDef to OMRS RelationshipDef are one-to-n, so never a prefix
            EntityProxy ep1 = RelationshipMapping.getEntityProxyForObject(
                    atlasRepositoryConnector,
                    typeDefStore,
                    atlasRepositoryConnector.getEntityByGUID(atlasEp1.getGuid(), true, true),
                    null,
                    userId
            );
            EntityProxy ep2 = RelationshipMapping.getEntityProxyForObject(
                    atlasRepositoryConnector,
                    typeDefStore,
                    atlasRepositoryConnector.getEntityByGUID(atlasEp2.getGuid(), true, true),
                    null,
                    userId
            );

            omrsRelationship = RelationshipMapping.getRelationship(
                    atlasRepositoryConnector,
                    typeDefStore,
                    attributeDefStore,
                    atlasRelationshipType,
                    atlasRelationship.getGuid(),
                    atlasRelationship.getStatus(),
                    ep1,
                    ep2,
                    atlasRelationship.getCreatedBy(),
                    atlasRelationship.getUpdatedBy(),
                    atlasRelationship.getCreateTime(),
                    atlasRelationship.getUpdateTime(),
                    atlasRelationship.getAttributes());

        }

        return omrsRelationship;

    }

    /**
     * Create a mapped relationship based on the provided criteria
     *
     * @param atlasRepositoryConnector connectivity to an Apache Atlas environment
     * @param typeDefStore store of TypeDef mappings
     * @param attributeDefStore store of AttributeTypeDef mappings
     * @param atlasRelationshipType the type of the Atlas relationship to map
     * @param relationshipGUID the GUID of the relationship
     * @param relationshipStatus the status of the relationship
     * @param ep1 the proxy to map to endpoint 1
     * @param ep2 the proxy to map to endpoint 2
     * @param createdBy the relationship creator
     * @param updatedBy the relationship updator
     * @param createTime the time the relationship was created
     * @param updateTime the time the relationship was updated
     * @param atlasRelationshipProperties the properties to set on the relationship
     * @return Relationship
     * @throws RepositoryErrorException when unable to map the Relationship
     */
    public static Relationship getRelationship(ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector,
                                               TypeDefStore typeDefStore,
                                               AttributeTypeDefStore attributeDefStore,
                                               String atlasRelationshipType,
                                               String relationshipGUID,
                                               AtlasRelationship.Status relationshipStatus,
                                               EntityProxy ep1,
                                               EntityProxy ep2,
                                               String createdBy,
                                               String updatedBy,
                                               Date createTime,
                                               Date updateTime,
                                               Map<String, Object> atlasRelationshipProperties) throws RepositoryErrorException {

        final String methodName = "getRelationship";
        OMRSRepositoryHelper omrsRepositoryHelper = atlasRepositoryConnector.getRepositoryHelper();
        String repositoryName = atlasRepositoryConnector.getRepositoryName();

        String omrsRelationshipType = typeDefStore.getMappedOMRSTypeDefName(atlasRelationshipType, null);

        Relationship omrsRelationship = RelationshipMapping.getSkeletonRelationship(
                atlasRepositoryConnector,
                (RelationshipDef) typeDefStore.getTypeDefByName(omrsRelationshipType)
        );

        omrsRelationship.setGUID(relationshipGUID);
        omrsRelationship.setMetadataCollectionId(atlasRepositoryConnector.getMetadataCollectionId());
        switch (relationshipStatus) {
            case ACTIVE:
                omrsRelationship.setStatus(InstanceStatus.ACTIVE);
                break;
            case DELETED:
                omrsRelationship.setStatus(InstanceStatus.DELETED);
                break;
            default:
                if (log.isWarnEnabled()) {
                    log.warn("Unhandled relationship status, defaulting to ACTIVE: {}", relationshipStatus);
                }
                omrsRelationship.setStatus(InstanceStatus.ACTIVE);
                break;
        }
        omrsRelationship.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
        // These don't appear to be retained by Atlas, so defaulting to values based on update time
        omrsRelationship.setVersion(updateTime.getTime());
        omrsRelationship.setCreateTime(createTime);
        omrsRelationship.setCreatedBy(createdBy);
        omrsRelationship.setUpdatedBy(updatedBy);
        omrsRelationship.setUpdateTime(updateTime);

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
                    RelationshipMapping.class.getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        if (atlasRelationshipProperties != null) {

            Map<String, TypeDefAttribute> relationshipAttributeMap = typeDefStore.getAllTypeDefAttributesForName(omrsRelationshipType);
            // TODO: currently all mappings from Atlas RelationshipDef to OMRS RelationshipDef are one-to-n, so never a prefix
            Map<String, String> atlasToOmrsProperties = typeDefStore.getPropertyMappingsForAtlasTypeDef(atlasRelationshipType, null);
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
                        if (log.isWarnEnabled()) {
                            log.warn("No OMRS attribute {} defined for asset type {} -- skipping mapping.", omrsProperty, omrsRelationshipType);
                        }
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
     * @param entityPrefix the prefix used for the entity, if it is a generated entity (null if not generated)
     * @param userId the user through which to retrieve the EntityProxy (unused)
     * @return EntityProxy
     */
    static EntityProxy getEntityProxyForObject(ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector,
                                               TypeDefStore typeDefStore,
                                               AtlasEntity.AtlasEntityWithExtInfo fullAtlasObj,
                                               String entityPrefix,
                                               String userId) {

        final String methodName = "getEntityProxyForObject";
        AtlasEntity atlasObj = fullAtlasObj.getEntity();

        EntityProxy entityProxy = null;
        if (atlasObj != null) {

            String repositoryName = atlasRepositoryConnector.getRepositoryName();
            OMRSRepositoryHelper repositoryHelper = atlasRepositoryConnector.getRepositoryHelper();
            String metadataCollectionId = atlasRepositoryConnector.getMetadataCollectionId();

            String atlasTypeName = atlasObj.getTypeName();
            String omrsTypeDefName = typeDefStore.getMappedOMRSTypeDefName(atlasTypeName, entityPrefix);

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
                    EntityMappingAtlas2OMRS.class.getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        return relationship;

    }

}
