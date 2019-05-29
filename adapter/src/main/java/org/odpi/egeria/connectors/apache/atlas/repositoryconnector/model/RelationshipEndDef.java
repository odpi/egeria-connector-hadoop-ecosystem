/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonIgnoreProperties(ignoreUnknown=true)
@JsonTypeName("RelationshipEndDef")
public class RelationshipEndDef {

    private String type;
    private String name;
    private boolean isContainer;
    private String cardinality;
    private boolean isLegacyAttribute;

    @JsonProperty("type") public String getType() { return this.type; }
    @JsonProperty("type") public void setType(String type) { this.type = type; }

    @JsonProperty("name") public String getName() { return this.name; }
    @JsonProperty("name") public void setName(String name) { this.name = name; }

    @JsonProperty("isContainer") public boolean isContainer() { return this.isContainer; }
    @JsonProperty("isContainer") public void setContainer(boolean isContainer) { this.isContainer = isContainer; }

    @JsonProperty("cardinality") public String getCardinality() { return this.cardinality; }
    @JsonProperty("cardinality") public void setCardinality(String cardinality) { this.cardinality = cardinality; }

    @JsonProperty("isLegacyAttribute") public boolean isLegacyAttribute() { return this.isLegacyAttribute; }
    @JsonProperty("isLegacyAttribute") public void setLegacyAttribute(boolean isLegacyAttribute) { this.isLegacyAttribute = isLegacyAttribute; }

}
