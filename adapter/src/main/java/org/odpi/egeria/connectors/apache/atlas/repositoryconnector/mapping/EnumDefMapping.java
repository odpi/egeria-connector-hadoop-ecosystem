/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping;

import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.odpi.egeria.connectors.apache.atlas.auditlog.ApacheAtlasOMRSErrorCode;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.ApacheAtlasOMRSRepositoryConnector;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.EnumElementDef;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores.AttributeTypeDefStore;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.EnumDef;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.RepositoryErrorException;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.TypeDefNotSupportedException;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Class that generically handles converting between Apache Atlas and OMRS Enum (Attribute)TypeDefs.
 */
public abstract class EnumDefMapping {

    private EnumDefMapping() {
        // Do nothing...
    }

    /**
     * Add the provided enumeration definition to Apache Atlas.
     *
     * @param omrsEnumDef the OMRS enumeration definition to add
     * @param attributeDefStore the attribute definition store to which to add
     * @param atlasRepositoryConnector connectivity to the Atlas environment
     * @throws TypeDefNotSupportedException if the enumeration definition cannot be supported
     */
    public static void addEnumToAtlas(EnumDef omrsEnumDef,
                                      AttributeTypeDefStore attributeDefStore,
                                      ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector)
            throws TypeDefNotSupportedException {

        final String methodName = "addEnumToAtlas";

        String omrsTypeDefName = omrsEnumDef.getName();

        // Map base properties
        AtlasEnumDef enumTypeDef = new AtlasEnumDef();
        enumTypeDef.setGuid(omrsEnumDef.getGUID());
        enumTypeDef.setName(omrsTypeDefName);
        enumTypeDef.setServiceType("omrs");
        enumTypeDef.setCreatedBy("ODPi Egeria (OMRS)");
        enumTypeDef.setCreateTime(new Date());
        enumTypeDef.setVersion(omrsEnumDef.getVersion());
        enumTypeDef.setDescription(omrsEnumDef.getDescription());

        List<EnumElementDef> omrsElements = omrsEnumDef.getElementDefs();
        if (omrsElements != null) {
            for (EnumElementDef omrsElement : omrsElements) {
                AtlasEnumDef.AtlasEnumElementDef atlasElement = new AtlasEnumDef.AtlasEnumElementDef();
                atlasElement.setValue(omrsElement.getValue());
                atlasElement.setDescription(omrsElement.getDescription());
                atlasElement.setOrdinal(omrsElement.getOrdinal());
                enumTypeDef.addElement(atlasElement);
            }
        }

        AtlasTypesDef atlasTypesDef = new AtlasTypesDef();
        List<AtlasEnumDef> enumList = new ArrayList<>();
        enumList.add(enumTypeDef);
        atlasTypesDef.setEnumDefs(enumList);

        try {
            atlasRepositoryConnector.createTypeDef(atlasTypesDef);
            attributeDefStore.addTypeDef(omrsEnumDef);
        } catch (AtlasServiceException e) {
            attributeDefStore.addUnimplementedTypeDef(omrsEnumDef);
            ApacheAtlasOMRSErrorCode errorCode = ApacheAtlasOMRSErrorCode.TYPEDEF_NOT_SUPPORTED;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(omrsTypeDefName, atlasRepositoryConnector.getServerName());
            throw new TypeDefNotSupportedException(errorCode.getHTTPErrorCode(),
                    EnumDefMapping.class.getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction(),
                    e);
        }

    }

}
