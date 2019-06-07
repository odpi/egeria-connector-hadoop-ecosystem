/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping;

import org.apache.atlas.model.instance.*;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.ApacheAtlasOMRSRepositoryConnector;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores.AttributeTypeDefStore;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores.TypeDefStore;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.SequencingOrder;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.*;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.RelationshipDef;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDef;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDefAttribute;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper;
import org.odpi.openmetadata.repositoryservices.ffdc.OMRSErrorCode;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.RepositoryErrorException;
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
    private AttributeTypeDefStore attributeDefStore;
    private AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo;
    private AtlasEntity atlasEntity;
    private String userId;

    /**
     * Mapping itself must be initialized with various objects.
     *
     * @param atlasRepositoryConnector connectivity to an Apache Atlas repository
     * @param typeDefStore the store of mapped TypeDefs for the Atlas repository
     * @param attributeDefStore the store of mapped AttributeTypeDefs for the Atlas repository
     * @param instance the Atlas entity to be mapped
     * @param userId the user through which to do the mapping
     */
    public EntityMapping(ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector,
                         TypeDefStore typeDefStore,
                         AttributeTypeDefStore attributeDefStore,
                         AtlasEntity.AtlasEntityWithExtInfo instance,
                         String userId) {
        this.atlasRepositoryConnector = atlasRepositoryConnector;
        this.typeDefStore = typeDefStore;
        this.attributeDefStore = attributeDefStore;
        this.atlasEntityWithExtInfo = instance;
        this.atlasEntity = atlasEntityWithExtInfo.getEntity();
        this.userId = userId;
    }

    /**
     * Retrieve the mapped OMRS EntitySummary from the Apache Atlas EntityInstance used to construct this mapping object.
     *
     * @return EntitySummary
     * @throws RepositoryErrorException when unable to retrieve the EntitySummary
     */
    public EntitySummary getEntitySummary() throws RepositoryErrorException {
        EntitySummary summary = getSkeletonEntitySummary();
        setModAndVersionDetails(summary);
        addClassifications(summary);
        return summary;
    }

    /**
     * Retrieve the mapped OMRS EntityDetail from the Apache Atlas EntityInstance used to construct this mapping object.
     *
     * @return EntityDetail
     * @throws RepositoryErrorException when unable to retrieve the EntityDetail
     */
    public EntityDetail getEntityDetail() throws RepositoryErrorException {

        final String methodName = "getEntityDetail";
        String atlasTypeDefName = atlasEntity.getTypeName();
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
            Map<String, Object> atlasProperties = atlasEntity.getAttributes();
            if (atlasProperties != null) {
                Set<String> alreadyMapped = new HashSet<>();
                for (Map.Entry<String, String> property : atlasToOmrsProperties.entrySet()) {
                    String atlasProperty = property.getKey();
                    String omrsProperty = property.getValue();
                    if (omrsAttributeMap.containsKey(omrsProperty)) {
                        TypeDefAttribute typeDefAttribute = omrsAttributeMap.get(omrsProperty);
                        instanceProperties = AttributeMapping.addPropertyToInstance(omrsRepositoryHelper,
                                repositoryName,
                                typeDefAttribute,
                                instanceProperties,
                                attributeDefStore,
                                atlasProperties.get(atlasProperty),
                                methodName);
                        if (instanceProperties.getPropertyValue(omrsProperty) != null) {
                            alreadyMapped.add(atlasProperty);
                        }
                    } else {
                        if (log.isWarnEnabled()) {
                            log.warn("No OMRS attribute {} defined for asset type {} -- skipping mapping.", omrsProperty, omrsTypeDefName);
                        }
                    }
                }

                // And map any other simple (non-relationship) properties that are not otherwise mapped into 'additionalProperties'
                Map<String, String> additionalProperties = new HashMap<>();

                Set<String> nonRelationshipSet = atlasProperties.keySet();

                // Remove all of the already-mapped properties from our list of non-relationship properties
                nonRelationshipSet.removeAll(alreadyMapped);

                // Iterate through the remaining property names, and add them to a map
                // Note that because 'additionalProperties' is a string-to-string map, we will just convert everything
                // to strings (even arrays of values, we'll concatenate into a single string)
                for (String propertyName : nonRelationshipSet) {
                    Object propertyValue = atlasProperties.get(propertyName);
                    if (propertyValue != null) {
                        additionalProperties.put(propertyName, propertyValue.toString());
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
            addClassifications(detail);

        }

        return detail;

    }

    /**
     * Retrieves relationships for this entity based on the provided criteria.
     *
     * @param relationshipTypeGUID the OMRS GUID of the relationship TypeDef to which to limit the results
     * @param fromRelationshipElement the starting element for multiple pages of relationships
     * @param sequencingProperty the property by which to order results (or null)
     * @param sequencingOrder the ordering sequence to use for ordering results
     * @param pageSize the number of results to include per page
     * @return {@code List<Relationship>}
     * @throws RepositoryErrorException when unable to retrieve the mapped Relationships
     */
    public List<Relationship> getRelationships(String relationshipTypeGUID,
                                               int fromRelationshipElement,
                                               String sequencingProperty,
                                               SequencingOrder sequencingOrder,
                                               int pageSize) throws RepositoryErrorException {

        final String methodName = "getRelationships";
        List<Relationship> omrsRelationships = new ArrayList<>();
        OMRSRepositoryHelper omrsRepositoryHelper = atlasRepositoryConnector.getRepositoryHelper();
        String repositoryName = atlasRepositoryConnector.getRepositoryName();

        // We have all the relationships from Atlas (no way to limit the query), so we will iterate
        // through them all
        Map<String, Object> atlasRelationships = atlasEntity.getRelationshipAttributes();
        for (Map.Entry<String, Object> atlasRelationship : atlasRelationships.entrySet()) {

            String atlasPropertyName = atlasRelationship.getKey();

            // TODO: we could avoid iterating through all of the assignments
            //  if we store the mapping between relationship property name and type in the TypeDefStore
            //  (but presumably the expensive bit is retrieving all the relationships, which happens anyway...)
            List<LinkedHashMap> relationshipAssignments = null;
            Object atlasRelationshipValue = atlasRelationship.getValue();
            if (atlasRelationshipValue instanceof List) {
                relationshipAssignments = (List<LinkedHashMap>) atlasRelationshipValue;
            } else if (atlasRelationshipValue instanceof LinkedHashMap) {
                // Not sure why Atlas doesn't just use the Jackson derserialization feature that makes single values into
                // lists, and end up with everything as an AtlasRelatedObjectId, but they don't...
                relationshipAssignments = new ArrayList<>();
                relationshipAssignments.add((LinkedHashMap) atlasRelationshipValue);
            }

            if (relationshipAssignments != null) {
                for (LinkedHashMap valueToTranslate : relationshipAssignments) {

                    AtlasRelatedObjectId relationshipAssignment = new AtlasRelatedObjectId(valueToTranslate);
                    String atlasRelationshipType = relationshipAssignment.getRelationshipType();
                    String omrsRelationshipType = typeDefStore.getMappedOMRSTypeDefName(atlasRelationshipType);

                    if (omrsRelationshipType != null) {

                        TypeDef omrsTypeDef = typeDefStore.getTypeDefByName(omrsRelationshipType);
                        String omrsTypeDefGuid = omrsTypeDef.getGUID();

                        // Only include the relationship if we are including all or those that match this type GUID
                        if (relationshipTypeGUID == null || omrsTypeDefGuid.equals(relationshipTypeGUID)) {

                            // TODO: see about incorporating into / re-using RelationshipMapping.getRelationship (heavy overlap)
                            Relationship omrsRelationship = RelationshipMapping.getSkeletonRelationship(
                                    atlasRepositoryConnector,
                                    (RelationshipDef) omrsTypeDef
                            );
                            omrsRelationship.setGUID(relationshipAssignment.getRelationshipGuid());
                            omrsRelationship.setMetadataCollectionId(atlasRepositoryConnector.getMetadataCollectionId());
                            switch (relationshipAssignment.getRelationshipStatus()) {
                                case ACTIVE:
                                    omrsRelationship.setStatus(InstanceStatus.ACTIVE);
                                    break;
                                case DELETED:
                                    omrsRelationship.setStatus(InstanceStatus.DELETED);
                                    break;
                                default:
                                    if (log.isWarnEnabled()) {
                                        log.warn("Unhandled relationship status, defaulting to ACTIVE: {}", relationshipAssignment.getRelationshipStatus());
                                    }
                                    omrsRelationship.setStatus(InstanceStatus.ACTIVE);
                                    break;
                            }
                            omrsRelationship.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
                            // These don't appear to be retained by Atlas, so defaulting to values based on update time
                            omrsRelationship.setVersion(atlasEntity.getUpdateTime().getTime());
                            omrsRelationship.setCreateTime(atlasEntity.getUpdateTime());
                            omrsRelationship.setCreatedBy(atlasEntity.getCreatedBy());
                            omrsRelationship.setUpdatedBy(atlasEntity.getUpdatedBy());
                            omrsRelationship.setUpdateTime(atlasEntity.getUpdateTime());

                            TypeDefStore.Endpoint endpoint = typeDefStore.getMappedEndpointFromAtlasName(atlasRelationshipType, atlasPropertyName);

                            EntityProxy epSelf = RelationshipMapping.getEntityProxyForObject(
                                    atlasRepositoryConnector,
                                    typeDefStore,
                                    atlasEntityWithExtInfo,
                                    userId
                            );

                            String otherEndGuid = relationshipAssignment.getGuid();

                            EntityProxy epOther = RelationshipMapping.getEntityProxyForObject(
                                    atlasRepositoryConnector,
                                    typeDefStore,
                                    atlasRepositoryConnector.getEntityByGUID(otherEndGuid, true, true),
                                    userId
                            );

                            if (epSelf != null && epOther != null) {
                                switch (endpoint) {
                                    case ONE:
                                        omrsRelationship.setEntityOneProxy(epSelf);
                                        omrsRelationship.setEntityTwoProxy(epOther);
                                        break;
                                    case TWO:
                                        omrsRelationship.setEntityOneProxy(epOther);
                                        omrsRelationship.setEntityTwoProxy(epSelf);
                                        break;
                                    default:
                                        OMRSErrorCode errorCode = OMRSErrorCode.INVALID_RELATIONSHIP_ENDS;
                                        String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                                                repositoryName,
                                                omrsRelationshipType,
                                                atlasPropertyName,
                                                null);
                                        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                                EntityMapping.class.getName(),
                                                methodName,
                                                errorMessage,
                                                errorCode.getSystemAction(),
                                                errorCode.getUserAction());
                                }
                            } else {
                                OMRSErrorCode errorCode = OMRSErrorCode.INVALID_RELATIONSHIP_ENDS;
                                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                                        repositoryName,
                                        omrsRelationshipType,
                                        atlasPropertyName,
                                        null);
                                throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                        EntityMapping.class.getName(),
                                        methodName,
                                        errorMessage,
                                        errorCode.getSystemAction(),
                                        errorCode.getUserAction());
                            }

                            AtlasStruct attrsOnAtlasReln = relationshipAssignment.getRelationshipAttributes();
                            if (attrsOnAtlasReln != null) {

                                Map<String, Object> atlasRelationshipProperties = attrsOnAtlasReln.getAttributes();
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
                                            if (log.isWarnEnabled()) {
                                                log.warn("No OMRS attribute {} defined for asset type {} -- skipping mapping.", omrsProperty, omrsRelationshipType);
                                            }
                                        }
                                    }

                                }
                                omrsRelationship.setProperties(omrsRelationshipProperties);

                            }
                            omrsRelationships.add(omrsRelationship);

                        }
                    }
                }
            }

        }

        // Now sort the results, if requested
        Comparator<Relationship> comparator = null;
        if (sequencingOrder != null) {
            switch (sequencingOrder) {
                case GUID:
                    comparator = Comparator.comparing(Relationship::getGUID);
                    break;
                case LAST_UPDATE_OLDEST:
                    comparator = Comparator.comparing(Relationship::getUpdateTime);
                    break;
                case LAST_UPDATE_RECENT:
                    comparator = Comparator.comparing(Relationship::getUpdateTime).reversed();
                    break;
                case CREATION_DATE_OLDEST:
                    comparator = Comparator.comparing(Relationship::getCreateTime);
                    break;
                case CREATION_DATE_RECENT:
                    comparator = Comparator.comparing(Relationship::getCreateTime).reversed();
                    break;
                case PROPERTY_ASCENDING:
                    if (sequencingProperty != null) {
                        comparator = (a, b) -> {
                            InstanceProperties p1 = a.getProperties();
                            InstanceProperties p2 = b.getProperties();
                            InstancePropertyValue v1 = null;
                            InstancePropertyValue v2 = null;
                            if (p1 != null) {
                                v1 = p1.getPropertyValue(sequencingProperty);
                            }
                            if (p2 != null) {
                                v2 = p2.getPropertyValue(sequencingProperty);
                            }
                            return AttributeMapping.compareInstanceProperty(v1, v2);
                        };
                    }
                    break;
                case PROPERTY_DESCENDING:
                    if (sequencingProperty != null) {
                        comparator = (b, a) -> {
                            InstanceProperties p1 = a.getProperties();
                            InstanceProperties p2 = b.getProperties();
                            InstancePropertyValue v1 = null;
                            InstancePropertyValue v2 = null;
                            if (p1 != null) {
                                v1 = p1.getPropertyValue(sequencingProperty);
                            }
                            if (p2 != null) {
                                v2 = p2.getPropertyValue(sequencingProperty);
                            }
                            return AttributeMapping.compareInstanceProperty(v1, v2);
                        };
                    }
                    break;
                default:
                    // Do nothing -- no sorting
                    break;
            }
            if (comparator != null) {
                omrsRelationships.sort(comparator);
            }
        }

        // And finally limit the results, if requested
        int endOfPageMarker = Math.min(fromRelationshipElement + pageSize, omrsRelationships.size());
        if (fromRelationshipElement != 0 || endOfPageMarker < omrsRelationships.size()) {
            omrsRelationships = omrsRelationships.subList(fromRelationshipElement, endOfPageMarker);
        }

        return (omrsRelationships.isEmpty() ? null : omrsRelationships);

    }

    /**
     * Create the base skeleton of an EntitySummary, irrespective of the specific Apache Atlas object.
     *
     * @return EntitySummary
     */
    private EntitySummary getSkeletonEntitySummary() {
        EntitySummary summary = new EntitySummary();
        String guid = atlasEntity.getGuid();
        summary.setGUID(guid);
        summary.setInstanceURL(getInstanceURL(guid));
        return summary;
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
            switch(atlasEntity.getStatus()) {
                case ACTIVE:
                    detail.setStatus(InstanceStatus.ACTIVE);
                    break;
                case DELETED:
                    detail.setStatus(InstanceStatus.DELETED);
                    break;
                default:
                    log.warn("Unhandled status: {}", atlasEntity.getStatus());
                    break;
            }
            String guid = atlasEntity.getGuid();
            detail.setGUID(guid);
            detail.setInstanceURL(getInstanceURL(guid));
            setModAndVersionDetails(detail);
        } catch (TypeErrorException e) {
            log.error("Unable to get skeleton detail entity.", e);
        }

        return detail;

    }

    /**
     * Retrieve an API-accessible instance URL based on the GUID of an entity.
     *
     * @param guid the guid of the entity
     * @return String
     */
    private String getInstanceURL(String guid) {
        return atlasRepositoryConnector.getBaseURL() + ApacheAtlasOMRSRepositoryConnector.EP_ENTITY + guid;
    }

    /**
     * Set the creation, modification and version details of the object.
     *
     * @param omrsObj the OMRS object (EntitySummary or EntityDetail)
     */
    private void setModAndVersionDetails(EntitySummary omrsObj) {
        omrsObj.setCreatedBy(atlasEntity.getCreatedBy());
        omrsObj.setCreateTime(atlasEntity.getCreateTime());
        omrsObj.setUpdatedBy(atlasEntity.getUpdatedBy());
        omrsObj.setUpdateTime(atlasEntity.getUpdateTime());
        omrsObj.setVersion(atlasEntity.getVersion());
    }

    /**
     * Add any classifications: since Atlas does not come with any pre-defined Classifications we will assume
     * that any that exist are OMRS-created and therefore are one-to-one mappings to OMRS classifications
     * (but we will check that the classification is a known OMRS classification before proceeding)
     *
     * @param omrsObj the OMRS object (EntitySummary or EntityDetail)
     * @throws RepositoryErrorException when unable to add the classifications
     */
    private void addClassifications(EntitySummary omrsObj) throws RepositoryErrorException {

        final String methodName = "addClassifications";

        List<Classification> classifications = new ArrayList<>();
        List<AtlasClassification> classificationAssignments = atlasEntity.getClassifications();
        if (classificationAssignments != null) {

            OMRSRepositoryHelper omrsRepositoryHelper = atlasRepositoryConnector.getRepositoryHelper();
            String repositoryName = atlasRepositoryConnector.getRepositoryName();
            String atlasTypeDefName = atlasEntity.getTypeName();
            String omrsTypeDefName = typeDefStore.getMappedOMRSTypeDefName(atlasTypeDefName);

            for (AtlasClassification classificationAssignment : classificationAssignments) {

                String atlasClassificationName = classificationAssignment.getTypeName();
                // Remember that the atlasClassificationName == the omrsClassificationName
                TypeDef classificationDef = typeDefStore.getTypeDefByName(atlasClassificationName);

                if (classificationDef != null) {
                    Map<String, Object> atlasClassificationProperties = classificationAssignment.getAttributes();
                    Map<String, TypeDefAttribute> classificationAttributeMap = typeDefStore.getAllTypeDefAttributesForName(atlasClassificationName);
                    InstanceProperties omrsClassificationProperties = new InstanceProperties();
                    if (atlasClassificationProperties != null) {

                        for (Map.Entry<String, Object> property : atlasClassificationProperties.entrySet()) {
                            String propertyName = property.getKey();
                            Object propertyValue = property.getValue();
                            if (classificationAttributeMap.containsKey(propertyName)) {
                                TypeDefAttribute typeDefAttribute = classificationAttributeMap.get(propertyName);
                                omrsClassificationProperties = AttributeMapping.addPropertyToInstance(omrsRepositoryHelper,
                                        repositoryName,
                                        typeDefAttribute,
                                        omrsClassificationProperties,
                                        attributeDefStore,
                                        propertyValue,
                                        methodName);
                            } else {
                                if (log.isWarnEnabled()) { log.warn("No OMRS attribute {} defined for asset type {} -- skipping mapping.", propertyName, atlasClassificationName); }
                            }
                        }

                    }
                    try {
                        // TODO: currently hard-coded to ASSIGNED classifications, need to also handle PROPAGATED
                        Classification classification = atlasRepositoryConnector.getRepositoryHelper().getNewClassification(
                                repositoryName,
                                userId,
                                atlasClassificationName,
                                omrsTypeDefName,
                                ClassificationOrigin.ASSIGNED,
                                null,
                                omrsClassificationProperties
                        );
                        // Setting classification mod details based on the overall entity (nothing more fine-grained in Atlas)
                        classification.setCreatedBy(omrsObj.getCreatedBy());
                        classification.setCreateTime(omrsObj.getCreateTime());
                        classification.setUpdatedBy(omrsObj.getUpdatedBy());
                        classification.setUpdateTime(omrsObj.getUpdateTime());
                        classification.setVersion(omrsObj.getUpdateTime().getTime());
                        classifications.add(classification);
                    } catch (TypeErrorException e) {
                        log.error("Unable to create a new classification.", e);
                        OMRSErrorCode errorCode = OMRSErrorCode.INVALID_CLASSIFICATION_FOR_ENTITY;
                        String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(
                                atlasClassificationName,
                                omrsTypeDefName);
                        throw new RepositoryErrorException(errorCode.getHTTPErrorCode(),
                                EntityMapping.class.getName(),
                                methodName,
                                errorMessage,
                                errorCode.getSystemAction(),
                                errorCode.getUserAction());
                    }
                } else {
                    log.warn("Classification {} unknown to repository -- skipping.", atlasClassificationName);
                }
            }
        }
        if (!classifications.isEmpty()) {
            omrsObj.setClassifications(classifications);
        }

    }

}
