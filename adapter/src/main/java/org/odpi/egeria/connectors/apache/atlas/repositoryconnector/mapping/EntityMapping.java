/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.ApacheAtlasOMRSRepositoryConnector;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.instances.ClassificationAssignment;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.instances.EntityInstance;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.instances.RelationshipAssignment;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.instances.RelationshipAssignmentAttributes;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores.AttributeTypeDefStore;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores.TypeDefStore;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.SequencingOrder;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.*;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.PrimitiveDefCategory;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.RelationshipDef;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDef;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDefAttribute;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper;
import org.odpi.openmetadata.repositoryservices.ffdc.OMRSErrorCode;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.RepositoryErrorException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.TypeErrorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * Class that generically handles converting an Apache Atlas EntityInstance object into an OMRS EntityDetail object.
 */
public class EntityMapping {

    private static final Logger log = LoggerFactory.getLogger(EntityMapping.class);

    private ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector;
    private TypeDefStore typeDefStore;
    private AttributeTypeDefStore attributeDefStore;
    private EntityInstance atlasEntityInstance;
    private String userId;

    public EntityMapping(ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector,
                         TypeDefStore typeDefStore,
                         AttributeTypeDefStore attributeDefStore,
                         EntityInstance instance,
                         String userId) {
        this.atlasRepositoryConnector = atlasRepositoryConnector;
        this.typeDefStore = typeDefStore;
        this.attributeDefStore = attributeDefStore;
        this.atlasEntityInstance = instance;
        this.userId = userId;
    }

    /**
     * Retrieve the mapped OMRS EntityDetail from the Apache Atlas EntityInstance used to construct this mapping object.
     *
     * @return EntityDetail
     */
    public EntityDetail getEntityDetail() throws RepositoryErrorException {

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
                    instanceProperties = AttributeMapping.addPropertyToInstance(omrsRepositoryHelper,
                            repositoryName,
                            typeDefAttribute,
                            instanceProperties,
                            attributeDefStore,
                            atlasProperties.get(atlasProperty),
                            methodName);
                    if (instanceProperties.getPropertyValue(atlasProperty) != null) {
                        alreadyMapped.add(atlasProperty);
                    }
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

            // Setup any classifications: since Atlas does not come with any pre-defined Classifications we will
            // assume that any that exist are OMRS-created and therefore are one-to-one mappings to OMRS classifications
            // (but we will check that the classification is a known OMRS classification before proceeding)
            List<Classification> classifications = new ArrayList<>();
            List<ClassificationAssignment> classificationAssignments = atlasEntityInstance.getClassifications();
            if (classificationAssignments != null) {

                for (ClassificationAssignment classificationAssignment : classificationAssignments) {

                    String atlasClassificationName = classificationAssignment.getTypeName();
                    TypeDef classificationDef = typeDefStore.getTypeDefByName(atlasClassificationName);

                    if (classificationDef != null) {
                        Map<String, String> atlasClassificationProperties = classificationAssignment.getAttributes();
                        Map<String, TypeDefAttribute> classificationAttributeMap = typeDefStore.getAllTypeDefAttributesForName(atlasClassificationName);
                        InstanceProperties omrsClassificationProperties = new InstanceProperties();
                        if (atlasClassificationProperties != null) {

                            for (Map.Entry<String, String> property : atlasClassificationProperties.entrySet()) {
                                String propertyName = property.getKey();
                                String propertyValue = property.getValue();
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
                            classification.setCreatedBy(detail.getCreatedBy());
                            classification.setCreateTime(detail.getCreateTime());
                            classification.setUpdatedBy(detail.getUpdatedBy());
                            classification.setUpdateTime(detail.getUpdateTime());
                            classification.setVersion(detail.getUpdateTime().getTime());
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
                detail.setClassifications(classifications);
            }

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
     * @throws RepositoryErrorException
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
        Map<String, List<RelationshipAssignment>> atlasRelationships = atlasEntityInstance.getRelationshipAttributes();
        for (Map.Entry<String, List<RelationshipAssignment>> atlasRelationship : atlasRelationships.entrySet()) {

            String atlasPropertyName = atlasRelationship.getKey();

            // TODO: we could avoid iterating through all of the assignments
            //  if we store the mapping between relationship property name and type in the TypeDefStore
            //  (but presumably the expensive bit is retrieving all the relationships, which happens anyway...)
            List<RelationshipAssignment> relationshipAssignments = atlasRelationship.getValue();
            for (RelationshipAssignment relationshipAssignment : relationshipAssignments) {
                String atlasRelationshipType = relationshipAssignment.getRelationshipType();
                String omrsRelationshipType = typeDefStore.getMappedOMRSTypeDefName(atlasRelationshipType);
                if (omrsRelationshipType != null) {
                    TypeDef omrsTypeDef = typeDefStore.getTypeDefByName(omrsRelationshipType);
                    String omrsTypeDefGuid = omrsTypeDef.getGUID();
                    // Only include the relationship if we are including all or those that match this type GUID
                    if (relationshipTypeGUID == null || omrsTypeDefGuid.equals(relationshipTypeGUID)) {

                        Relationship omrsRelationship = getSkeletonRelationship((RelationshipDef) omrsTypeDef);
                        omrsRelationship.setGUID(relationshipAssignment.getRelationshipGuid());
                        omrsRelationship.setMetadataCollectionId(atlasRepositoryConnector.getMetadataCollectionId());
                        if (!relationshipAssignment.getRelationshipStatus().equals("ACTIVE")) {
                            log.warn("Unhandled relationship status, defaulting to ACTIVE: {}", relationshipAssignment.getRelationshipStatus());
                        }
                        omrsRelationship.setStatus(InstanceStatus.ACTIVE);
                        omrsRelationship.setInstanceProvenanceType(InstanceProvenanceType.LOCAL_COHORT);
                        // TODO: these don't appear to be retained by Atlas, so defaulting to values based on update time
                        omrsRelationship.setVersion(atlasEntityInstance.getUpdateTime().getTime());
                        omrsRelationship.setCreateTime(atlasEntityInstance.getUpdateTime());
                        omrsRelationship.setCreatedBy(atlasEntityInstance.getCreatedBy());
                        omrsRelationship.setUpdatedBy(atlasEntityInstance.getUpdatedBy());
                        omrsRelationship.setUpdateTime(atlasEntityInstance.getUpdateTime());

                        TypeDefStore.Endpoint endpoint = typeDefStore.getMappedEndpointFromAtlasName(atlasRelationshipType, atlasPropertyName);

                        EntityProxy epSelf = EntityMapping.getEntityProxyForObject(
                                atlasRepositoryConnector,
                                typeDefStore,
                                atlasEntityInstance,
                                userId
                        );

                        String otherEndGuid = relationshipAssignment.getGuid();

                        EntityProxy epOther = EntityMapping.getEntityProxyForObject(
                                atlasRepositoryConnector,
                                typeDefStore,
                                atlasRepositoryConnector.getEntityByGUID(otherEndGuid, true),
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

                        RelationshipAssignmentAttributes attrsOnAtlasReln = relationshipAssignment.getRelationshipAttributes();
                        if (attrsOnAtlasReln != null) {

                            Map<String, String> atlasRelationshipProperties = attrsOnAtlasReln.getAttributes();
                            Map<String, TypeDefAttribute> relationshipAttributeMap = typeDefStore.getAllTypeDefAttributesForName(omrsRelationshipType);
                            InstanceProperties omrsRelationshipProperties = new InstanceProperties();
                            if (atlasRelationshipProperties != null) {

                                for (Map.Entry<String, String> property : atlasRelationshipProperties.entrySet()) {
                                    String propertyName = property.getKey();
                                    String propertyValue = property.getValue();
                                    if (relationshipAttributeMap.containsKey(propertyName)) {
                                        TypeDefAttribute typeDefAttribute = relationshipAttributeMap.get(propertyName);
                                        omrsRelationshipProperties = AttributeMapping.addPropertyToInstance(omrsRepositoryHelper,
                                                repositoryName,
                                                typeDefAttribute,
                                                omrsRelationshipProperties,
                                                attributeDefStore,
                                                propertyValue,
                                                methodName);
                                    } else {
                                        if (log.isWarnEnabled()) { log.warn("No OMRS attribute {} defined for asset type {} -- skipping mapping.", propertyName, atlasRelationshipType); }
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
                            return compareInstanceProperty(v1, v2);
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
                            return compareInstanceProperty(v1, v2);
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
     * Retrieves an EntityProxy object for the provided Apache Atlas object.
     *
     * @param atlasRepositoryConnector OMRS connector to the Apache Atlas repository
     * @param typeDefStore store of mapped TypeDefs
     * @param atlasObj the Apache Atlas object for which to retrieve an EntityProxy
     * @param userId the user through which to retrieve the EntityProxy (unused)
     * @return EntityProxy
     */
    public static EntityProxy getEntityProxyForObject(ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector,
                                                      TypeDefStore typeDefStore,
                                                      EntityInstance atlasObj,
                                                      String userId) {

        final String methodName = "getEntityProxyForObject";

        EntityProxy entityProxy = null;
        if (atlasObj != null) {

            String repositoryName = atlasRepositoryConnector.getRepositoryName();
            OMRSRepositoryHelper repositoryHelper = atlasRepositoryConnector.getRepositoryHelper();
            String metadataCollectionId = atlasRepositoryConnector.getMetadataCollectionId();

            String atlasTypeName = atlasObj.getTypeName();
            String omrsTypeDefName = typeDefStore.getMappedOMRSTypeDefName(atlasTypeName);

            String qualifiedName = null;
            Map<String, String> attributes = atlasObj.getAttributes();
            if (attributes.containsKey("qualifiedName")) {
                qualifiedName = attributes.get("qualifiedName");
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
     * Comparator input for sorting based on an InstancePropertyValue. Note that this will assume that both v1 and v2
     * are the same type of property value (eg. both same type of primitive)
     *
     * @param v1 first value to compare
     * @param v2 second value to compare
     * @return int
     */
    private int compareInstanceProperty(InstancePropertyValue v1, InstancePropertyValue v2) {

        int result = 0;
        if (v1 == v2) {
            result = 0;
        } else if (v1 == null) {
            result = -1;
        } else if (v2 == null) {
            result = 1;
        } else {

            InstancePropertyCategory category = v1.getInstancePropertyCategory();
            switch (category) {
                case PRIMITIVE:
                    PrimitivePropertyValue pv1 = (PrimitivePropertyValue) v1;
                    PrimitivePropertyValue pv2 = (PrimitivePropertyValue) v2;
                    PrimitiveDefCategory primitiveCategory = pv1.getPrimitiveDefCategory();
                    switch (primitiveCategory) {
                        case OM_PRIMITIVE_TYPE_INT:
                            result = ((Integer) pv1.getPrimitiveValue() - (Integer) pv2.getPrimitiveValue());
                            break;
                        case OM_PRIMITIVE_TYPE_BYTE:
                            result = ((Byte) pv1.getPrimitiveValue()).compareTo((Byte) pv2.getPrimitiveValue());
                            break;
                        case OM_PRIMITIVE_TYPE_CHAR:
                            result = ((Character) pv1.getPrimitiveValue()).compareTo((Character) pv2.getPrimitiveValue());
                            break;
                        case OM_PRIMITIVE_TYPE_STRING:
                            result = ((String) pv1.getPrimitiveValue()).compareTo((String) pv2.getPrimitiveValue());
                            break;
                        case OM_PRIMITIVE_TYPE_DATE:
                            result = ((Date) pv1.getPrimitiveValue()).compareTo((Date) pv2.getPrimitiveValue());
                            break;
                        case OM_PRIMITIVE_TYPE_LONG:
                            result = ((Long) pv1.getPrimitiveValue()).compareTo((Long) pv2.getPrimitiveValue());
                            break;
                        case OM_PRIMITIVE_TYPE_FLOAT:
                            result = ((Float) pv1.getPrimitiveValue()).compareTo((Float) pv2.getPrimitiveValue());
                            break;
                        case OM_PRIMITIVE_TYPE_SHORT:
                            result = ((Short) pv1.getPrimitiveValue()).compareTo((Short) pv2.getPrimitiveValue());
                            break;
                        case OM_PRIMITIVE_TYPE_DOUBLE:
                            result = ((Double) pv1.getPrimitiveValue()).compareTo((Double) pv2.getPrimitiveValue());
                            break;
                        case OM_PRIMITIVE_TYPE_BOOLEAN:
                            result = ((Boolean) pv1.getPrimitiveValue()).compareTo((Boolean) pv2.getPrimitiveValue());
                            break;
                        case OM_PRIMITIVE_TYPE_BIGDECIMAL:
                            result = ((BigDecimal) pv1.getPrimitiveValue()).compareTo((BigDecimal) pv2.getPrimitiveValue());
                            break;
                        case OM_PRIMITIVE_TYPE_BIGINTEGER:
                            result = ((BigInteger) pv1.getPrimitiveValue()).compareTo((BigInteger) pv2.getPrimitiveValue());
                            break;
                        default:
                            result = pv1.getPrimitiveValue().toString().compareTo(pv2.getPrimitiveValue().toString());
                            break;
                    }
                    break;
                default:
                    log.warn("Unhandled instance value type for comparison: {}", category);
                    break;
            }

        }
        return result;

    }

    /**
     * Create the base skeleton of a Relationship, irrespective of the specific Apache Atlas object.
     *
     * @param omrsRelationshipDef the OMRS RelationshipDef for which to create a skeleton Relationship
     * @return Relatoinship
     * @throws RepositoryErrorException
     */
    private Relationship getSkeletonRelationship(RelationshipDef omrsRelationshipDef) throws RepositoryErrorException {

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
