/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown=true)
@JsonTypeName("CLASSIFICATION")
public class ClassificationTypeDef extends TypeDefHeader {

    private List<AttributeDef> attributeDefs;
    private List<String> superTypes;
    private List<String> subTypes;
    private List<String> entityTypes;

    @JsonProperty("attributeDefs") public List<AttributeDef> getAttributeDefs() { return this.attributeDefs; }
    @JsonProperty("attributeDefs") public void setAttributeDefs(List<AttributeDef> attributeDefs) { this.attributeDefs = attributeDefs; }

    @JsonProperty("superTypes") public List<String> getSuperTypes() { return this.superTypes; }
    @JsonProperty("superTypes") public void setSuperTypes(List<String> superTypes) { this.superTypes = superTypes; }

    @JsonProperty("subTypes") public List<String> getSubTypes() { return this.subTypes; }
    @JsonProperty("subTypes") public void setSubTypes(List<String> subTypes) { this.subTypes = subTypes; }

    @JsonProperty("entityTypes") public List<String> getEntityTypes() { return this.entityTypes; }
    @JsonProperty("entityTypes") public void setEntityTypes(List<String> entityTypes) { this.entityTypes = entityTypes; }

}
