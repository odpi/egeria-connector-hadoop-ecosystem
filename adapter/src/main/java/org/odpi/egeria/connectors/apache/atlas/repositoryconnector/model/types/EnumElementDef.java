/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.types;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonIgnoreProperties(ignoreUnknown=true)
@JsonTypeName("EnumElementDef")
public class EnumElementDef {

    private String value;
    private String description;
    private int ordinal;

    @JsonProperty("value") public String getValue() { return this.value; }
    @JsonProperty("value") public void setValue(String value) { this.value = value; }

    @JsonProperty("description") public String getDescription() { return this.description; }
    @JsonProperty("description") public void setDescription(String description) { this.description = description; }

    @JsonProperty("ordinal") public int getOrdinal() { return this.ordinal; }
    @JsonProperty("ordinal") public void setOrdinal(int ordinal) { this.ordinal = ordinal; }

}
