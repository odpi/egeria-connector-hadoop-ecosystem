/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.types;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.types.AttributeDef;

@JsonIgnoreProperties(ignoreUnknown=true)
@JsonTypeName("RelationshipAttributeDef")
public class RelationshipAttributeDef extends AttributeDef {

    private String relationshipTypeName;
    private boolean isLegacyAttribute;

    @JsonProperty("relationshipTypeName") public String getRelationshipTypeName() { return this.relationshipTypeName; }
    @JsonProperty("relatioshipTypeName") public void setRelationshipTypeName(String relationshipTypeName) { this.relationshipTypeName = relationshipTypeName; }

    @JsonProperty("isLegacyAttribute") public boolean isLegacyAttribute() { return this.isLegacyAttribute; }
    @JsonProperty("isLegacyAttribute") public void setLegacyAttribute(boolean isLegacyAttribute) { this.isLegacyAttribute = isLegacyAttribute; }

}
