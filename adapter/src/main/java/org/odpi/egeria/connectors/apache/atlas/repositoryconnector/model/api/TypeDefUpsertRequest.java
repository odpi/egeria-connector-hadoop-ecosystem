/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.types.*;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown=true)
@JsonTypeName("TypeDefUpsertRequest")
public class TypeDefUpsertRequest {

    private List<ClassificationTypeDef> classificationDefs;
    private List<EntityTypeDef> entityDefs;
    private List<EnumTypeDef> enumDefs;
    private List<RelationshipTypeDef> relationshipDefs;
    private List<StructTypeDef> structDefs;

    @JsonProperty("classificationDefs") public List<ClassificationTypeDef> getClassificationDefs() { return this.classificationDefs; }
    @JsonProperty("classificationDefs") public void setClassificationDefs(List<ClassificationTypeDef> classificationDefs) { this.classificationDefs = classificationDefs; }

    @JsonProperty("entityDefs") public List<EntityTypeDef> getEntityDefs() { return this.entityDefs; }
    @JsonProperty("entityDefs") public void setEntityDefs(List<EntityTypeDef> entityDefs) { this.entityDefs = entityDefs; }

    @JsonProperty("enumDefs") public List<EnumTypeDef> getEnumDefs() { return this.enumDefs; }
    @JsonProperty("enumDefs") public void setEnumDefs(List<EnumTypeDef> enumDefs) { this.enumDefs = enumDefs; }

    @JsonProperty("relationshipDefs") public List<RelationshipTypeDef> getRelationshipDefs() { return this.relationshipDefs; }
    @JsonProperty("relationshipDefs") public void setRelationshipDefs(List<RelationshipTypeDef> relationshipDefs) { this.relationshipDefs = relationshipDefs; }

    @JsonProperty("structDefs") public List<StructTypeDef> getStructDefs() { return this.structDefs; }
    @JsonProperty("structDefs") public void setStructDefs(List<StructTypeDef> structDefs) { this.structDefs = structDefs; }

}
