/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping;

import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.ApacheAtlasOMRSRepositoryConnector;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores.AttributeTypeDefStore;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores.TypeDefStore;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.TypeDefNotSupportedException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Class that generically handles converting between Apache Atlas and OMRS Entity TypeDefs.
 */
public abstract class EntityDefMapping extends BaseTypeDefMapping {

    private EntityDefMapping() {
        // Do nothing...
    }

    /**
     * Adds the provided OMRS type definition to Apache Atlas (if possible), or throws a TypeDefNotSupportedException
     * if not possible.
     *
     * @param omrsEntityDef the OMRS EntityDef to add to Apache Atlas
     * @param typeDefStore the store of mapped / implemented TypeDefs in Apache Atlas
     * @param attributeDefStore the store of mapped / implemented TypeDefAttributes in Apache Atlas
     * @param atlasRepositoryConnector connectivity to the Apache Atlas environment
     * @throws TypeDefNotSupportedException when entity cannot be fully represented in Atlas
     */
    public static void addEntityTypeToAtlas(EntityDef omrsEntityDef,
                                            TypeDefStore typeDefStore,
                                            AttributeTypeDefStore attributeDefStore,
                                            ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector) throws TypeDefNotSupportedException {

        final String methodName = "addEntityTypeToAtlas";

        String omrsTypeDefName = omrsEntityDef.getName();
        boolean fullyCovered = true;

        // Map base properties
        AtlasEntityDef entityTypeDef = new AtlasEntityDef();
        setupBaseMapping(omrsEntityDef, entityTypeDef);

        // Map entity-specific properties: supertype (if any)
        TypeDefLink supertype = omrsEntityDef.getSuperType();
        if (supertype != null) {
            // TODO: assumes all mapped superTypes remain actual defs (never generated, so never a prefix)
            String atlasSupertypeName = typeDefStore.getMappedAtlasTypeDefName(supertype.getName(), null);
            if (atlasSupertypeName != null) {
                Set<String> supertypes = new HashSet<>();
                supertypes.add(atlasSupertypeName);
                entityTypeDef.setSuperTypes(supertypes);
            } else {
                fullyCovered = false;
            }
        }

        // Note: we do not need to setup relationship attributes here, as they will be setup automatically
        // when we define the relationship typedefs

        fullyCovered = fullyCovered && setupPropertyMappings(omrsEntityDef, entityTypeDef, attributeDefStore);

        if (fullyCovered) {
            // Only create the entity if we can fully model it
            AtlasTypesDef atlasTypesDef = new AtlasTypesDef();
            List<AtlasEntityDef> entityList = new ArrayList<>();
            entityList.add(entityTypeDef);
            atlasTypesDef.setEntityDefs(entityList);
            atlasRepositoryConnector.createTypeDef(atlasTypesDef);
            typeDefStore.addTypeDef(omrsEntityDef);
        } else {
            // Otherwise, we'll drop it as unimplemented
            typeDefStore.addUnimplementedTypeDef(omrsEntityDef);
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
