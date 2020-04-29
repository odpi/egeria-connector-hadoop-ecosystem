/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping;

import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.odpi.egeria.connectors.apache.atlas.auditlog.ApacheAtlasOMRSErrorCode;
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
public abstract class ClassificationDefMapping extends BaseTypeDefMapping {

    private static final Logger log = LoggerFactory.getLogger(ClassificationDefMapping.class);

    private ClassificationDefMapping() {
        // Do nothing...
    }

    /**
     * Adds the provided OMRS type definition to Apache Atlas (if possible), or throws a TypeDefNotSupportedException
     * if not possible.
     *
     * @param omrsClassificationDef the OMRS ClassificationDef to add to Apache Atlas
     * @param typeDefStore the store of mapped / implemented TypeDefs in Apache Atlas
     * @param attributeDefStore the store of mapped / implemented AttributeTypeDefs in Apache Atlas
     * @param atlasRepositoryConnector connectivity to the Apache Atlas environment
     * @throws TypeDefNotSupportedException when classification cannot be fully represented in Atlas
     */
    public static void addClassificationTypeToAtlas(ClassificationDef omrsClassificationDef,
                                                    TypeDefStore typeDefStore,
                                                    AttributeTypeDefStore attributeDefStore,
                                                    ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector) throws TypeDefNotSupportedException {

        final String methodName = "addClassificationTypeToAtlas";

        String omrsTypeDefName = omrsClassificationDef.getName();
        boolean fullyCovered = true;

        // Map base properties
        AtlasClassificationDef classificationTypeDef = new AtlasClassificationDef();
        setupBaseMapping(omrsClassificationDef, classificationTypeDef);

        // Map classification-specific properties
        Set<String> entitiesForAtlas = new HashSet<>();
        List<TypeDefLink> validEntities = omrsClassificationDef.getValidEntityDefs();
        for (TypeDefLink typeDefLink : validEntities) {
            String omrsEntityName = typeDefLink.getName();
            // TODO: assumes all classifications remain one-to-one (never generated, so never a prefix)
            String atlasEntityName = typeDefStore.getMappedAtlasTypeDefName(omrsEntityName, null);
            if (atlasEntityName != null) {
                entitiesForAtlas.add(atlasEntityName);
            }
        }
        if (entitiesForAtlas.isEmpty()) {
            log.warn("No relevant Atlas entities found for classification: {}", omrsTypeDefName);
            fullyCovered = false;
        }
        classificationTypeDef.setEntityTypes(entitiesForAtlas);

        fullyCovered = fullyCovered && setupPropertyMappings(omrsClassificationDef, classificationTypeDef, attributeDefStore);

        if (fullyCovered) {
            // Only create the classification if we can fully model it
            AtlasTypesDef atlasTypesDef = new AtlasTypesDef();
            List<AtlasClassificationDef> classificationList = new ArrayList<>();
            classificationList.add(classificationTypeDef);
            atlasTypesDef.setClassificationDefs(classificationList);
            try {
                atlasRepositoryConnector.createTypeDef(atlasTypesDef);
                typeDefStore.addTypeDef(omrsClassificationDef);
            } catch (AtlasServiceException e) {
                typeDefStore.addUnimplementedTypeDef(omrsClassificationDef);
                raiseTypeDefNotSupportedException(ApacheAtlasOMRSErrorCode.TYPEDEF_NOT_SUPPORTED, methodName, e, omrsClassificationDef.getName(), atlasRepositoryConnector.getServerName());
            }
        } else {
            // Otherwise, we'll drop it as unimplemented
            typeDefStore.addUnimplementedTypeDef(omrsClassificationDef);
            raiseTypeDefNotSupportedException(ApacheAtlasOMRSErrorCode.TYPEDEF_NOT_SUPPORTED, methodName, null, omrsClassificationDef.getName(), atlasRepositoryConnector.getServerName());
        }

    }

    /**
     * Throws a TypeDefNotSupportedException using the provided parameters.
     * @param errorCode the error code for the exception
     * @param methodName the method throwing the exception
     * @param cause the underlying cause of the exception (if any, otherwise null)
     * @param params any parameters for formatting the error message
     * @throws TypeDefNotSupportedException always
     */
    private static void raiseTypeDefNotSupportedException(ApacheAtlasOMRSErrorCode errorCode, String methodName, Throwable cause, String ...params) throws TypeDefNotSupportedException {
        if (cause == null) {
            throw new TypeDefNotSupportedException(errorCode.getMessageDefinition(params),
                    ClassificationDefMapping.class.getName(),
                    methodName);
        } else {
            throw new TypeDefNotSupportedException(errorCode.getMessageDefinition(params),
                    ClassificationDefMapping.class.getName(),
                    methodName,
                    cause);
        }
    }

}
