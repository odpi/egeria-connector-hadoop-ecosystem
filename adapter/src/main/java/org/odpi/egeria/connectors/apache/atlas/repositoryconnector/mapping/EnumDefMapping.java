/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping;

import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.ApacheAtlasOMRSRepositoryConnector;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.types.EnumElementDef;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.types.EnumTypeDef;
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
        EnumTypeDef enumTypeDef = new EnumTypeDef();
        enumTypeDef.setGuid(omrsEnumDef.getGUID());
        enumTypeDef.setName(omrsTypeDefName);
        enumTypeDef.setServiceType("omrs");
        enumTypeDef.setCategory("ENUM");
        enumTypeDef.setCreatedBy("ODPi Egeria (OMRS)");
        enumTypeDef.setCreateTime(new Date());
        enumTypeDef.setVersion(omrsEnumDef.getVersion());
        enumTypeDef.setTypeVersion("1.1");
        enumTypeDef.setDescription(omrsEnumDef.getDescription());

        List<EnumElementDef> atlasElements = new ArrayList<>();
        List<org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.EnumElementDef> omrsElements = omrsEnumDef.getElementDefs();
        if (omrsElements != null) {
            for (org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.EnumElementDef omrsElement : omrsElements) {
                EnumElementDef atlasElement = new EnumElementDef();
                atlasElement.setValue(omrsElement.getValue());
                atlasElement.setDescription(omrsElement.getDescription());
                atlasElement.setOrdinal(omrsElement.getOrdinal());
                atlasElements.add(atlasElement);
            }
        }
        enumTypeDef.setElementDefs(atlasElements);

        atlasRepositoryConnector.createTypeDef(enumTypeDef);
        attributeDefStore.addTypeDef(omrsEnumDef);

    }

}
