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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Class that generically handles converting between Apache Atlas and OMRS Entity TypeDefs.
 */
public abstract class EntityDefMapping extends BaseTypeDefMapping {

    private static final Logger log = LoggerFactory.getLogger(EntityDefMapping.class);

    /**
     * Adds the provided OMRS type definition to Apache Atlas (if possible), or throws a TypeDefNotSupportedException
     * if not possible.
     *
     * @param omrsEntityDef the OMRS EntityDef to add to Apache Atlas
     * @param typeDefStore the store of mapped / implemented TypeDefs in Apache Atlas
     * @param attributeDefStore the store of mapped / implemented TypeDefAttributes in Apache Atlas
     * @param atlasRepositoryConnector connectivity to the Apache Atlas environment
     * @throws TypeDefNotSupportedException
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
            String atlasSupertypeName = typeDefStore.getMappedAtlasTypeDefName(supertype.getName());
            if (atlasSupertypeName != null) {
                Set<String> supertypes = new HashSet<>();
                supertypes.add(atlasSupertypeName);
                entityTypeDef.setSuperTypes(supertypes);
            } else {
                fullyCovered = false;
            }
        }

        // TODO: is setting relationship attributes here required, or will they be added when relationship typedefs
        //  are created? (If needed, probably need to be added as an update to the entity typedef, as part of the
        //  relationship creation process?)

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
