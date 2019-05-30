/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.types;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown=true)
@JsonTypeName("ENUM")
public class EnumTypeDef extends TypeDefHeader {

    private List<EnumElementDef> elementDefs;

    @JsonProperty("elementDefs") public List<EnumElementDef> getElementDefs() { return this.elementDefs; }
    @JsonProperty("elementDefs") public void setElementDefs(List<EnumElementDef> elementDefs) { this.elementDefs = elementDefs; }

}
