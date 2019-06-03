/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping;

import org.apache.atlas.model.typedef.AtlasEnumDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.ApacheAtlasOMRSRepositoryConnector;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.EnumElementDef;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores.AttributeTypeDefStore;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.EnumDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Class that generically handles converting between Apache Atlas and OMRS Enum (Attribute)TypeDefs.
 */
public abstract class EnumDefMapping {

    private static final Logger log = LoggerFactory.getLogger(EnumDefMapping.class);

    public static void addEnumToAtlas(EnumDef omrsEnumDef,
                                      AttributeTypeDefStore attributeDefStore,
                                      ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector) {

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

        atlasRepositoryConnector.createTypeDef(atlasTypesDef);
        attributeDefStore.addTypeDef(omrsEnumDef);

    }

}
