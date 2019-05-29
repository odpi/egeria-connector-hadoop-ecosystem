/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown=true)
@JsonTypeName("RELATIONSHIP")
public class RelationshipTypeDef extends TypeDefHeader {

    private List<AttributeDef> attributeDefs;
    private String relationshipCategory;
    private String propagateTags;
    private RelationshipEndDef endDef1;
    private RelationshipEndDef endDef2;

    @JsonProperty("attributeDefs") public List<AttributeDef> getAttributeDefs() { return this.attributeDefs; }
    @JsonProperty("attributeDefs") public void setAttributeDefs(List<AttributeDef> attributeDefs) { this.attributeDefs = attributeDefs; }

    @JsonProperty("relationshipCategory") public String getRelationshipCategory() { return this.relationshipCategory; }
    @JsonProperty("relationshipCategory") public void setRelationshipCategory(String relationshipCategory) { this.relationshipCategory = relationshipCategory; }

    @JsonProperty("propagateTags") public String getPropagateTags() { return this.propagateTags; }
    @JsonProperty("propagateTags") public void setPropagateTags(String propagateTags) { this.propagateTags = propagateTags; }

    @JsonProperty("endDef1") public RelationshipEndDef getEndDef1() { return this.endDef1; }
    @JsonProperty("endDef1") public void setEndDef1(RelationshipEndDef endDef1) { this.endDef1 = endDef1; }

    @JsonProperty("endDef2") public RelationshipEndDef getEndDef2() { return this.endDef2; }
    @JsonProperty("endDef2") public void setEndDef2(RelationshipEndDef endDef2) { this.endDef2 = endDef2; }

}
