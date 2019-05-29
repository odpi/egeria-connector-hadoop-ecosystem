/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;

/**
 * Represents a single TypeDef mapping, to safely parse TypeDefMappings.json resource.
 */
@JsonIgnoreProperties(ignoreUnknown=true)
@JsonTypeName("TypeDefMapping")
public class TypeDefMapping {

    /**
     * The name of the TypeDef within Apache Atlas.
     */
    private String atlas;

    /**
     * The name of the TypeDef within OMRS.
     */
    private String omrs;

    /**
     * An array of mappings between Apache Atlas and OMRS property names for the TypeDef.
     */
    private List<TypeDefMapping> propertyMappings;

    @JsonProperty("atlas") public String getAtlasName() { return this.atlas; }
    @JsonProperty("atlas") public void setAtlasName(String atlas) { this.atlas = atlas; }

    @JsonProperty("omrs") public String getOMRSName() { return this.omrs; }
    @JsonProperty("omrs") public void setOMRSName(String omrs) { this.omrs = omrs; }

    @JsonProperty("propertyMappings") public List<TypeDefMapping> getPropertyMappings() { return this.propertyMappings; }
    @JsonProperty("propertyMappings") public void setPropertyMappings(List<TypeDefMapping> propertyMappings) { this.propertyMappings = propertyMappings; }

}
