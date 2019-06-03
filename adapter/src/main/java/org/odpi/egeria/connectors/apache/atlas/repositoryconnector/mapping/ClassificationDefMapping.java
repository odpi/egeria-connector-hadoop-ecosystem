/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping;

import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.ApacheAtlasOMRSMetadataCollection;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.ApacheAtlasOMRSRepositoryConnector;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.types.AttributeDef;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.types.ClassificationTypeDef;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores.TypeDefStore;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.TypeDefNotSupportedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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
                                                ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector) throws TypeDefNotSupportedException {

        final String methodName = "addClassificationToAtlas";

        String omrsTypeDefName = omrsClassificationDef.getName();
        boolean fullyCovered = true;

        // Map base properties
        ClassificationTypeDef classificationTypeDef = new ClassificationTypeDef();
        classificationTypeDef.setGuid(omrsClassificationDef.getGUID());
        classificationTypeDef.setName(omrsTypeDefName);
        classificationTypeDef.setServiceType("omrs");
        classificationTypeDef.setCategory("CLASSIFICATION");
        classificationTypeDef.setCreatedBy(omrsClassificationDef.getCreatedBy());
        classificationTypeDef.setUpdatedBy(omrsClassificationDef.getUpdatedBy());
        classificationTypeDef.setCreateTime(omrsClassificationDef.getCreateTime());
        classificationTypeDef.setUpdateTime(omrsClassificationDef.getUpdateTime());
        classificationTypeDef.setVersion(omrsClassificationDef.getVersion());
        classificationTypeDef.setTypeVersion("1.0");
        classificationTypeDef.setDescription(omrsClassificationDef.getDescription());

        // Map classification-specific properties
        ArrayList<String> entitiesForAtlas = new ArrayList<>();
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

        ArrayList<AttributeDef> propertiesForAtlas = new ArrayList<>();
        List<TypeDefAttribute> omrsProperties = omrsClassificationDef.getPropertiesDefinition();
        if (omrsProperties != null) {
            log.info("List of properties is not null...");
            for (TypeDefAttribute typeDefAttribute : omrsProperties) {
                log.info(" ... checking property: {}", typeDefAttribute);
                AttributeDef atlasAttribute = new AttributeDef();
                AttributeCardinality omrsCardinality = typeDefAttribute.getAttributeCardinality();
                switch (omrsCardinality) {
                    case AT_MOST_ONE:
                    case ONE_ONLY:
                        atlasAttribute.setCardinality("SINGLE");
                        break;
                    case ANY_NUMBER_UNORDERED:
                    case AT_LEAST_ONE_UNORDERED:
                        atlasAttribute.setCardinality("SET");
                        break;
                    case ANY_NUMBER_ORDERED:
                    case AT_LEAST_ONE_ORDERED:
                        atlasAttribute.setCardinality("LIST");
                        break;
                    default:
                        fullyCovered = false;
                        log.warn("Unknown cardinality for OMRS property: {}", typeDefAttribute.getAttributeName());
                        break;
                }
                atlasAttribute.setDefaultValue(typeDefAttribute.getDefaultValue());
                atlasAttribute.setDescription(typeDefAttribute.getAttributeDescription());
                atlasAttribute.setIncludeInNotification(true);
                atlasAttribute.setIndexable(typeDefAttribute.isIndexable());
                int minValues = typeDefAttribute.getValuesMinCount();
                if (minValues >= 1) {
                    atlasAttribute.setOptional(false);
                } else {
                    atlasAttribute.setOptional(true);
                }
                atlasAttribute.setUnique(typeDefAttribute.isUnique());
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
                /*
                case ENUM_DEF:
                    EnumDef enumDef = (EnumDef) attributeTypeDef;
                    String omrsEnumName = enumDef.getName();
                    // TODO: Translate OMRS enum name into the corresponding Apache Atlas enum
                    atlasAttribute.setTypeName("??? name of Enum (eg. BlahBlahStatus) ???");
                    break; */
                    default:
                        fullyCovered = false;
                        log.warn("Unhandled attribute type for classification: {}", attributeTypeDef.getName());
                        break;
                }
                atlasAttribute.setValuesMinCount(minValues);
                atlasAttribute.setValuesMaxCount(typeDefAttribute.getValuesMaxCount());
                propertiesForAtlas.add(atlasAttribute);
            }
            classificationTypeDef.setAttributeDefs(propertiesForAtlas);
        }

        if (fullyCovered) {
            // Only create the classification if we can fully model it
            atlasRepositoryConnector.createTypeDef(classificationTypeDef);
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
