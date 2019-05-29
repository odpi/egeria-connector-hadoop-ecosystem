/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown=true)
@JsonTypeName("STRUCT")
public class StructTypeDef extends TypeDefHeader {

    private List<AttributeDef> attributeDefs;

    @JsonProperty("attributeDefs") public List<AttributeDef> getAttributeDefs() { return this.attributeDefs; }
    @JsonProperty("attributeDefs") public void setAttributeDefs(List<AttributeDef> attributeDefs) { this.attributeDefs = attributeDefs; }

}
