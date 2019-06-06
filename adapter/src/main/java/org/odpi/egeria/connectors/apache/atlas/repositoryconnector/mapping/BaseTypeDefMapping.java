/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping;

import org.apache.atlas.model.typedef.*;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores.AttributeTypeDefStore;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Class that generically handles converting between Apache Atlas and OMRS TypeDefs.
 */
public class BaseTypeDefMapping {

    private static final Logger log = LoggerFactory.getLogger(BaseTypeDefMapping.class);

    /**
     * Sets up the basic mapping of the provided OMRS type definition to the provided Apache Atlas type definition.
     *
     * @param omrsTypeDef the OMRS type definition
     * @param atlasTypeDef the Apache Atlas type definition
     */
    public static void setupBaseMapping(TypeDef omrsTypeDef,
                                        AtlasBaseTypeDef atlasTypeDef) {
        atlasTypeDef.setGuid(omrsTypeDef.getGUID());
        atlasTypeDef.setName(omrsTypeDef.getName());
        atlasTypeDef.setServiceType("omrs");
        atlasTypeDef.setCreatedBy(omrsTypeDef.getCreatedBy());
        atlasTypeDef.setUpdatedBy(omrsTypeDef.getUpdatedBy());
        atlasTypeDef.setCreateTime(omrsTypeDef.getCreateTime());
        atlasTypeDef.setUpdateTime(omrsTypeDef.getUpdateTime());
        atlasTypeDef.setVersion(omrsTypeDef.getVersion());
        atlasTypeDef.setDescription(omrsTypeDef.getDescription());
    }

    /**
     * Adds property definitions from the OMRS type definition to the provided Apache Atlas type definition, returning
     * a boolean indicating if the mapping was fully covered (true) or only partially (false).
     *
     * @param omrsTypeDef the OMRS type definition
     * @param atlasTypeDef the Apache Atlas type definition
     * @param attributeDefStore the store of mapped / implemented TypeDefAttributes in Apache Atlas
     * @return boolean - true if all of the properties could be mapped, otherwise false
     */
    public static boolean setupPropertyMappings(TypeDef omrsTypeDef,
                                                AtlasStructDef atlasTypeDef,
                                                AttributeTypeDefStore attributeDefStore) {

        boolean fullyCovered = true;

        // Map typedef-specific properties
        List<TypeDefAttribute> omrsProperties = omrsTypeDef.getPropertiesDefinition();
        if (omrsProperties != null) {
            if (log.isDebugEnabled()) { log.debug("List of properties is not null..."); }
            for (TypeDefAttribute typeDefAttribute : omrsProperties) {
                if (log.isDebugEnabled()) { log.debug(" ... checking property: {}", typeDefAttribute); }
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
                        atlasAttribute.setTypeName(primitiveDef.getName());
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
                    case COLLECTION:
                        CollectionDef collectionDef = (CollectionDef) attributeTypeDef;
                        CollectionDefCategory collectionDefCategory = collectionDef.getCollectionDefCategory();
                        switch (collectionDefCategory) {
                            case OM_COLLECTION_MAP:
                                List<PrimitiveDefCategory> mapArgs = collectionDef.getArgumentTypes();
                                atlasAttribute.setTypeName("map<" + getAtlasPrimitiveNameFromOMRSPrimitiveName(mapArgs.get(0).getName()) + "," + getAtlasPrimitiveNameFromOMRSPrimitiveName(mapArgs.get(1).getName()) + ">");
                                break;
                            case OM_COLLECTION_ARRAY:
                                List<PrimitiveDefCategory> arrayArgs = collectionDef.getArgumentTypes();
                                atlasAttribute.setTypeName("array<" + getAtlasPrimitiveNameFromOMRSPrimitiveName(arrayArgs.get(0).getName()) + ">");
                                break;
                            case OM_COLLECTION_STRUCT:
                                atlasAttribute.setTypeName("struct");
                                break;
                            default:
                                fullyCovered = false;
                                log.warn("Unhandled collection attribute type for typedef: {}", attributeTypeDef.getName());
                                break;
                        }
                        break;
                    default:
                        fullyCovered = false;
                        log.warn("Unhandled attribute type for classification: {}", attributeTypeDef.getName());
                        break;
                }
                atlasAttribute.setValuesMinCount(minValues);
                atlasAttribute.setValuesMaxCount(typeDefAttribute.getValuesMaxCount());
                atlasTypeDef.addAttribute(atlasAttribute);
            }
        }

        return fullyCovered;

    }

    /**
     * Converts the provided OMRS primitive type name into an Apache Atlas primitive type name.
     *
     * @param omrsPrimitiveName the OMRS primitive type name
     * @return String
     */
    private static String getAtlasPrimitiveNameFromOMRSPrimitiveName(String omrsPrimitiveName) {

        String atlasName = null;
        String lowercaseName = omrsPrimitiveName.toLowerCase();

        switch(lowercaseName) {
            case "boolean":
            case "string":
            case "long":
            case "int":
            case "date":
                atlasName = lowercaseName;
                break;
            case "object":
            case "char":
            case "byte":
            case "biginteger":
            case "bigdecimal":
                atlasName = "string";
                break;
            case "short":
                atlasName = "int";
                break;
            case "float":
            case "double":
                atlasName = "long";
                if (log.isWarnEnabled()) { log.warn("Actual type for OMRS is '{}', casting down to 'long' as no Atlas support decimal-based numbers?", lowercaseName); }
                break;
        }

        return atlasName;

    }

}
