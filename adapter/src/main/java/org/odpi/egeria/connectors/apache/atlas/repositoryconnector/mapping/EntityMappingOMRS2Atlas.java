/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping;

import org.apache.atlas.model.instance.*;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.ApacheAtlasOMRSErrorCode;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.ApacheAtlasOMRSRepositoryConnector;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores.AttributeTypeDefStore;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores.TypeDefStore;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.*;
import org.odpi.openmetadata.repositoryservices.ffdc.OMRSErrorCode;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Class that generically handles converting an OMRS EntityDetail object into an Apache Atlas AtlasEntity object.
 */
public class EntityMappingOMRS2Atlas {

    private static final Logger log = LoggerFactory.getLogger(EntityMappingOMRS2Atlas.class);

    private ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector;
    private TypeDefStore typeDefStore;
    private AttributeTypeDefStore attributeDefStore;
    private EntityDetail omrsEntity;
    private String userId;

    /**
     * Mapping itself must be initialized with various objects.
     *
     * @param atlasRepositoryConnector connectivity to an Apache Atlas repository
     * @param typeDefStore the store of mapped TypeDefs for the Atlas repository
     * @param attributeDefStore the store of mapped AttributeTypeDefs for the Atlas repository
     * @param instance the OMRS entity to be mapped
     * @param userId the user through which to do the mapping
     */
    public EntityMappingOMRS2Atlas(ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector,
                                   TypeDefStore typeDefStore,
                                   AttributeTypeDefStore attributeDefStore,
                                   EntityDetail instance,
                                   String userId) {
        this.atlasRepositoryConnector = atlasRepositoryConnector;
        this.typeDefStore = typeDefStore;
        this.attributeDefStore = attributeDefStore;
        this.omrsEntity = instance;
        this.userId = userId;
    }

    /**
     * Attempts to save a reference copy in Apache Atlas for the OMRS entity used to initialize this mapping.
     *
     * @return EntityOperation enumeration indicating operation that was taken on the entity, or null if there was a problem
     * @throws InvalidEntityException when any expected fields are missing on the OMRS entity
     * @throws PropertyErrorException one or more of the requested properties are not defined, or have different
     *                                characteristics in the TypeDef for this entity's type.
     * @throws EntityConflictException the new entity conflicts with an existing entity.
     */
    public EntityMutations.EntityOperation saveReferenceCopy() throws
            InvalidEntityException,
            PropertyErrorException,
            EntityConflictException {

        final String methodName = "saveReferenceCopy";

        AtlasEntity atlasEntity = getSkeletonAtlasEntity();

        List<String> missingFieldNames = new ArrayList<>();
        String guid = omrsEntity.getGUID();
        if (guid == null) {
            missingFieldNames.add("guid");
        } else {
            atlasEntity.setGuid(guid);
        }
        InstanceProvenanceType provenanceType = omrsEntity.getInstanceProvenanceType();
        if (provenanceType == null) {
            missingFieldNames.add("instanceProvenanceType");
        } else {
            atlasEntity.setProvenanceType(provenanceType.getOrdinal());
        }
        String metadataCollectionId = omrsEntity.getMetadataCollectionId();
        if (metadataCollectionId == null) {
            missingFieldNames.add("metadataCollectionId");
        } else {
            atlasEntity.setHomeId(metadataCollectionId);
        }
        checkForMissingFields(methodName, missingFieldNames);

        AtlasEntity.AtlasEntityWithExtInfo existingAtlasEntity = atlasRepositoryConnector.getEntityByGUID(guid, true, true, false);

        // If there is an existing entity, and it is not already a reference copy, we have a conflict
        boolean bCreate = (existingAtlasEntity == null);
        if (existingAtlasEntity != null && !existingAtlasEntity.getEntity().getHomeId().equals(metadataCollectionId)) {
            ApacheAtlasOMRSErrorCode errorCode = ApacheAtlasOMRSErrorCode.CONFLICTING_GUID_FOR_REFERENCE;
            String errorMessage = errorCode.getErrorMessageId()
                    + errorCode.getFormattedErrorMessage(methodName,
                    atlasRepositoryConnector.getRepositoryName(),
                    omrsEntity.getGUID());
            throw new EntityConflictException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        // Add any property values
        InstanceType omrsType = omrsEntity.getType();
        String omrsTypeName = omrsType.getTypeDefName();
        Map<String, String> omrsPropertyMap = typeDefStore.getPropertyMappingsForOMRSTypeDef(omrsTypeName);

        setProperties(
                omrsEntity.getProperties(),
                atlasEntity,
                omrsPropertyMap,
                omrsEntity.getType().getTypeDefName(),
                methodName
        );

        // Add any classifications
        addClassifications(atlasEntity);

        AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity);
        EntityMutations.EntityOperation opTaken = null;
        EntityMutationResponse result = atlasRepositoryConnector.saveEntity(atlasEntityWithExtInfo, bCreate);
        if (result != null) {
            Map<EntityMutations.EntityOperation, List<AtlasEntityHeader>> changes = result.getMutatedEntities();
            if (changes != null) {
                for (Map.Entry<EntityMutations.EntityOperation, List<AtlasEntityHeader>> change : changes.entrySet()) {
                    EntityMutations.EntityOperation operation = change.getKey();
                    List<AtlasEntityHeader> headers = change.getValue();
                    if (headers != null) {
                        for (AtlasEntityHeader header : headers) {
                            if (header.getGuid().equals(omrsEntity.getGUID())) {
                                opTaken = operation;
                            }
                        }
                    }
                }
            }
        }

        return opTaken;

    }

    /**
     * Adds any classifications defined on the OMRS entity this mapping was initialized with to the Atlas entity.
     *
     * @param atlasEntity the Apache Atlas entity to which to add any mapped classifications
     * @throws PropertyErrorException when the OMRS entity uses a status not recognized by Atlas
     */
    private void addClassifications(AtlasEntity atlasEntity) throws PropertyErrorException {

        final String methodName = "addClassifications";

        List<Classification> classifications = omrsEntity.getClassifications();
        if (classifications != null) {
            List<AtlasClassification> atlasClassifications = new ArrayList<>();
            for (Classification classification : classifications) {

                // Set up the most basic information
                AtlasClassification atlasClassification = getSkeletonAtlasClassification();

                // Set the basic type information
                InstanceType omrsType = classification.getType();
                String omrsTypeName = omrsType.getTypeDefName();
                String atlasTypeName = typeDefStore.getMappedAtlasTypeDefName(omrsTypeName);
                atlasClassification.setTypeName(atlasTypeName);
                Map<String, String> omrsPropertyMap = typeDefStore.getPropertyMappingsForOMRSTypeDef(omrsTypeName);

                // Add any property values
                setProperties(
                        classification.getProperties(),
                        atlasClassification,
                        omrsPropertyMap,
                        omrsTypeName,
                        methodName
                );

                atlasClassifications.add(atlasClassification);

            }
            atlasEntity.setClassifications(atlasClassifications);
        }

    }

    /**
     * Maps the OMRS properties provided to the Atlas object provided.
     *
     * @param omrsProperties the OMRS properties that should be mapped
     * @param atlasObj the Atlas object (Entity, Classification) on which to set the mapped properties
     * @param omrsPropertyMap the mapping of OMRS property names to Atlas property names
     * @param omrsTypeDefName the name of the OMRS type definition
     * @param methodName the name of the calling method
     * @throws PropertyErrorException when a property is not mapped or is otherwise unknown to Atlas
     */
    private void setProperties(InstanceProperties omrsProperties,
                               AtlasStruct atlasObj,
                               Map<String, String> omrsPropertyMap,
                               String omrsTypeDefName,
                               String methodName) throws PropertyErrorException {

        if (omrsProperties != null) {
            Map<String, InstancePropertyValue> omrsPropertyValues = omrsProperties.getInstanceProperties();
            if (omrsPropertyValues != null) {
                for (Map.Entry<String, InstancePropertyValue> omrsEntry : omrsPropertyValues.entrySet()) {
                    String omrsPropertyName = omrsEntry.getKey();
                    InstancePropertyValue omrsPropertyValue = omrsEntry.getValue();
                    String atlasPropertyName = omrsPropertyMap.get(omrsPropertyName);
                    if (atlasPropertyName != null) {
                        atlasObj.setAttribute(atlasPropertyName, AttributeMapping.getValueFromInstance(omrsPropertyValue, omrsTypeDefName, attributeDefStore));
                    } else {
                        // As this is only a reference copy, if we cannot capture the property value we will skip it and log
                        // a warning
                        ApacheAtlasOMRSErrorCode errorCode = ApacheAtlasOMRSErrorCode.PROPERTY_NOT_KNOWN_FOR_INSTANCE;
                        String errorMessage = errorCode.getErrorMessageId()
                                + errorCode.getFormattedErrorMessage(omrsPropertyName,
                                omrsTypeDefName,
                                atlasRepositoryConnector.getRepositoryName());
                        throw new PropertyErrorException(errorCode.getHTTPErrorCode(),
                                this.getClass().getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }
                }
            }
        }

    }

    /**
     * Retrieves the minimalistic mapping for an AtlasEntity, irrespective of the type definition of the entity.
     *
     * @return AtlasEntity
     * @throws InvalidEntityException when any expected fields are missing on the OMRS entity
     * @throws PropertyErrorException when the OMRS entity uses a status not recognized by Atlas
     */
    private AtlasEntity getSkeletonAtlasEntity() throws
            InvalidEntityException,
            PropertyErrorException {

        final String methodName = "getAtlasEntity";

        AtlasEntity atlasEntity = new AtlasEntity();

        List<String> missingFieldNames = new ArrayList<>();
        String createdBy = omrsEntity.getCreatedBy();
        if (createdBy == null) {
            missingFieldNames.add("createdBy");
        } else {
            atlasEntity.setCreatedBy(createdBy);
        }
        atlasEntity.setVersion(omrsEntity.getVersion());
        String updatedBy = omrsEntity.getUpdatedBy();
        if (omrsEntity.getVersion() > 1 && updatedBy == null) {
            missingFieldNames.add("updatedBy");
        } else {
            atlasEntity.setUpdatedBy(updatedBy);
        }
        Date createTime = omrsEntity.getCreateTime();
        if (createTime == null) {
            missingFieldNames.add("createTime");
        } else {
            atlasEntity.setCreateTime(createTime);
        }
        Date updateTime = omrsEntity.getUpdateTime();
        if (omrsEntity.getVersion() > 1 && updateTime == null) {
            missingFieldNames.add("updateTime");
        } else {
            atlasEntity.setUpdateTime(updateTime);
        }
        InstanceStatus status = omrsEntity.getStatus();
        if (status == null) {
            missingFieldNames.add("status");
        } else {
            switch(omrsEntity.getStatus()) {
                case ACTIVE:
                    atlasEntity.setStatus(AtlasEntity.Status.ACTIVE);
                    break;
                case DELETED:
                    atlasEntity.setStatus(AtlasEntity.Status.DELETED);
                    break;
                default:
                    OMRSErrorCode errorCode = OMRSErrorCode.BAD_INSTANCE_STATUS;
                    String errorMessage = errorCode.getErrorMessageId()
                            + errorCode.getFormattedErrorMessage(omrsEntity.getGUID(),
                            methodName,
                            atlasRepositoryConnector.getRepositoryName());
                    throw new PropertyErrorException(errorCode.getHTTPErrorCode(),
                            this.getClass().getName(),
                            methodName,
                            errorMessage,
                            errorCode.getSystemAction(),
                            errorCode.getUserAction());
            }
        }
        checkForMissingFields(methodName, missingFieldNames);

        return atlasEntity;

    }

    /**
     * Retrieves the minimalistic mapping for an AtlasClassification, irrespective of the type definition of it.
     *
     * @return AtlasClassification
     * @throws PropertyErrorException when the OMRS entity uses a status not recognized by Atlas
     */
    private AtlasClassification getSkeletonAtlasClassification() throws PropertyErrorException {

        final String methodName = "getSkeletonAtlasClassification";

        AtlasClassification atlasClassification = new AtlasClassification();

        atlasClassification.setEntityGuid(omrsEntity.getGUID());
        switch (omrsEntity.getStatus()) {
            case ACTIVE:
                atlasClassification.setEntityStatus(AtlasEntity.Status.ACTIVE);
                break;
            case DELETED:
                atlasClassification.setEntityStatus(AtlasEntity.Status.DELETED);
                break;
            default:
                OMRSErrorCode errorCode = OMRSErrorCode.BAD_INSTANCE_STATUS;
                String errorMessage = errorCode.getErrorMessageId()
                        + errorCode.getFormattedErrorMessage(omrsEntity.getGUID(),
                        methodName,
                        atlasRepositoryConnector.getRepositoryName());
                throw new PropertyErrorException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
        }

        return atlasClassification;

    }

    /**
     * Check for any missing fields that were identified.
     *
     * @param methodName the name of the calling method
     * @param missingFieldNames the list of field names that are missing (if any)
     * @throws InvalidEntityException when the list of provided field names is not empty
     */
    private void checkForMissingFields(String methodName, List<String> missingFieldNames) throws InvalidEntityException {
        if (!missingFieldNames.isEmpty()) {
            StringBuilder fieldMessage = new StringBuilder();
            fieldMessage.append("Supplied entity is missing the following fields: ");
            fieldMessage.append(String.join(", ", missingFieldNames));
            ApacheAtlasOMRSErrorCode errorCode = ApacheAtlasOMRSErrorCode.INVALID_INSTANCE_HEADER;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                    methodName,
                    atlasRepositoryConnector.getRepositoryName(),
                    fieldMessage.toString());
            throw new InvalidEntityException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
    }

}
