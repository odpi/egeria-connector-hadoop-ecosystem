/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping;

import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.ApacheAtlasOMRSRepositoryConnector;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores.AttributeTypeDefStore;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores.TypeDefStore;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.TypeDefNotSupportedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Class that generically handles converting between Apache Atlas and OMRS Classification TypeDefs.
 */
public abstract class ClassificationDefMapping {

    private static final Logger log = LoggerFactory.getLogger(ClassificationDefMapping.class);

    /**
     * Adds the provided OMRS type definition to Apache Atlas (if possible), or throws a TypeDefNotSupportedException
     * if not possible.
     *
     * @param omrsClassificationDef the OMRS ClassificationDef to add to Apache Atlas
     * @param typeDefStore the store of mapped / implemented TypeDefs in Apache Atlas
     * @param atlasRepositoryConnector connectivity to the Apache Atlas environment
     * @throws TypeDefNotSupportedException
     */
    public static void addClassificationToAtlas(ClassificationDef omrsClassificationDef,
                                                TypeDefStore typeDefStore,
                                                AttributeTypeDefStore attributeDefStore,
                                                ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector) throws TypeDefNotSupportedException {

        final String methodName = "addClassificationToAtlas";

        String omrsTypeDefName = omrsClassificationDef.getName();
        boolean fullyCovered = true;

        // Map base properties
        AtlasClassificationDef classificationTypeDef = new AtlasClassificationDef();
        classificationTypeDef.setGuid(omrsClassificationDef.getGUID());
        classificationTypeDef.setName(omrsTypeDefName);
        classificationTypeDef.setServiceType("omrs");
        classificationTypeDef.setCreatedBy(omrsClassificationDef.getCreatedBy());
        classificationTypeDef.setUpdatedBy(omrsClassificationDef.getUpdatedBy());
        classificationTypeDef.setCreateTime(omrsClassificationDef.getCreateTime());
        classificationTypeDef.setUpdateTime(omrsClassificationDef.getUpdateTime());
        classificationTypeDef.setVersion(omrsClassificationDef.getVersion());
        classificationTypeDef.setDescription(omrsClassificationDef.getDescription());

        // Map classification-specific properties
        Set<String> entitiesForAtlas = new HashSet<>();
        List<TypeDefLink> validEntities = omrsClassificationDef.getValidEntityDefs();
        for (TypeDefLink typeDefLink : validEntities) {
            String omrsEntityName = typeDefLink.getName();
            String atlasEntityName = typeDefStore.getMappedAtlasTypeDefName(omrsEntityName);
            if (atlasEntityName != null) {
                entitiesForAtlas.add(atlasEntityName);
            }
        }
        if (entitiesForAtlas.isEmpty()) {
            log.warn("No relevant Atlas entities found for classification: {}", omrsTypeDefName);
            fullyCovered = false;
        }
        classificationTypeDef.setEntityTypes(entitiesForAtlas);

        List<TypeDefAttribute> omrsProperties = omrsClassificationDef.getPropertiesDefinition();
        if (omrsProperties != null) {
            log.info("List of properties is not null...");
            for (TypeDefAttribute typeDefAttribute : omrsProperties) {
                log.info(" ... checking property: {}", typeDefAttribute);
                AtlasStructDef.AtlasAttributeDef atlasAttribute = new AtlasStructDef.AtlasAttributeDef();
                AttributeCardinality omrsCardinality = typeDefAttribute.getAttributeCardinality();
                switch (omrsCardinality) {
                    case AT_MOST_ONE:
                    case ONE_ONLY:
                        atlasAttribute.setCardinality(AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);
                        break;
                    case ANY_NUMBER_UNORDERED:
                    case AT_LEAST_ONE_UNORDERED:
                        atlasAttribute.setCardinality(AtlasStructDef.AtlasAttributeDef.Cardinality.SET);
                        break;
                    case ANY_NUMBER_ORDERED:
                    case AT_LEAST_ONE_ORDERED:
                        atlasAttribute.setCardinality(AtlasStructDef.AtlasAttributeDef.Cardinality.LIST);
                        break;
                    default:
                        fullyCovered = false;
                        log.warn("Unknown cardinality for OMRS property: {}", typeDefAttribute.getAttributeName());
                        break;
                }
                atlasAttribute.setDefaultValue(typeDefAttribute.getDefaultValue());
                atlasAttribute.setDescription(typeDefAttribute.getAttributeDescription());
                atlasAttribute.setIncludeInNotification(true);
                atlasAttribute.setIsIndexable(typeDefAttribute.isIndexable());
                int minValues = typeDefAttribute.getValuesMinCount();
                if (minValues >= 1) {
                    atlasAttribute.setIsOptional(false);
                } else {
                    atlasAttribute.setIsOptional(true);
                }
                atlasAttribute.setIsUnique(typeDefAttribute.isUnique());
                atlasAttribute.setName(typeDefAttribute.getAttributeName());
                AttributeTypeDef attributeTypeDef = typeDefAttribute.getAttributeType();
                switch (attributeTypeDef.getCategory()) {
                    case PRIMITIVE:
                        PrimitiveDef primitiveDef = (PrimitiveDef) attributeTypeDef;
                        PrimitiveDefCategory primitiveDefCategory = primitiveDef.getPrimitiveDefCategory();
                        switch (primitiveDefCategory) {
                            case OM_PRIMITIVE_TYPE_BOOLEAN:
                            case OM_PRIMITIVE_TYPE_BYTE:
                            case OM_PRIMITIVE_TYPE_SHORT:
                            case OM_PRIMITIVE_TYPE_INT:
                            case OM_PRIMITIVE_TYPE_FLOAT:
                            case OM_PRIMITIVE_TYPE_DOUBLE:
                            case OM_PRIMITIVE_TYPE_STRING:
                            case OM_PRIMITIVE_TYPE_DATE:
                                atlasAttribute.setTypeName(primitiveDefCategory.getName());
                                break;
                            default:
                                fullyCovered = false;
                                log.warn("Unhandled primitive attribute type for classification: {}", attributeTypeDef.getName());
                                break;
                        }
                        break;
                    case ENUM_DEF:
                        // Translate OMRS enum name into a mapped one (if there is a mapped one), otherwise default to
                        // the OMRS enum name (should have been created with the same name if it is not mapped to a
                        // pre-existing Atlas enum)
                        EnumDef enumDef = (EnumDef) attributeTypeDef;
                        String omrsEnumName = enumDef.getName();
                        String atlasEnumName = attributeDefStore.getMappedAtlasTypeDefName(omrsEnumName);
                        if (atlasEnumName == null) {
                            atlasEnumName = omrsEnumName;
                        }
                        atlasAttribute.setTypeName(atlasEnumName);
                        break;
                    default:
                        fullyCovered = false;
                        log.warn("Unhandled attribute type for classification: {}", attributeTypeDef.getName());
                        break;
                }
                atlasAttribute.setValuesMinCount(minValues);
                atlasAttribute.setValuesMaxCount(typeDefAttribute.getValuesMaxCount());
                classificationTypeDef.addAttribute(atlasAttribute);
            }
        }

        if (fullyCovered) {
            // Only create the classification if we can fully model it
            AtlasTypesDef atlasTypesDef = new AtlasTypesDef();
            List<AtlasClassificationDef> classificationList = new ArrayList<>();
            classificationList.add(classificationTypeDef);
            atlasTypesDef.setClassificationDefs(classificationList);
            atlasRepositoryConnector.createTypeDef(atlasTypesDef);
            typeDefStore.addTypeDef(omrsClassificationDef);
        } else {
            // Otherwise, we'll drop it as unimplemented
            typeDefStore.addUnimplementedTypeDef(omrsClassificationDef);
            throw new TypeDefNotSupportedException(
                    404,
                    ClassificationDefMapping.class.getName(),
                    methodName,
                    omrsTypeDefName + " is not supported.",
                    "",
                    "Request support through Egeria GitHub issue."
            );
        }

    }

}
