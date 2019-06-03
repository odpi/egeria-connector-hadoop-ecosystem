/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.instances;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown=true)
@JsonTypeName("RelationshipAssignmentAttributes")
public class RelationshipAssignmentAttributes {

    private String typeName;
    private Map<String, String> attributes;

    @JsonProperty("typeName") public String getTypeName() { return this.typeName; }
    @JsonProperty("typeName") public void setTypeName(String typeName) { this.typeName = typeName; }

    @JsonProperty("attributes") public Map<String, String> getAttributes() { return this.attributes; }
    @JsonProperty("attributes") public void setAttributes(Map<String, String> attributes) { this.attributes = attributes; }

}
