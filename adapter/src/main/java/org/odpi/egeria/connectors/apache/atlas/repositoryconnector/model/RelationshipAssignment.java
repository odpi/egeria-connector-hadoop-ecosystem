/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown=true)
@JsonTypeName("RelationshipAssignment")
public class RelationshipAssignment {

    private String guid;
    private String typeName;
    private String entityStatus;
    private String displayText;
    private String relationshipType;
    private String relationshipGuid;
    private String relationshipStatus;
    private Map<String, String> relationshipAttributes;

    @JsonProperty("guid") public String getGuid() { return this.guid; }
    @JsonProperty("guid") public void setGuid(String guid) { this.guid = guid; }

    @JsonProperty("typeName") public String getTypeName() { return this.typeName; }
    @JsonProperty("typeName") public void setTypeName(String typeName) { this.typeName = typeName; }

    @JsonProperty("entityStatus") public String getEntityStatus() { return this.entityStatus; }
    @JsonProperty("entityStatus") public void setEntityStatus(String entityStatus) { this.entityStatus = entityStatus; }

    @JsonProperty("displayText") public String getDisplayText() { return this.displayText; }
    @JsonProperty("displayText") public void setDisplayText(String displayText) { this.displayText = displayText; }

    @JsonProperty("relationshipType") public String getRelationshipType() { return this.relationshipType; }
    @JsonProperty("relationshipType") public void setRelationshipType(String relationshipType) { this.relationshipType = relationshipType; }

    @JsonProperty("relationshipGuid") public String getRelationshipGuid() { return this.relationshipGuid; }
    @JsonProperty("relationshipGuid") public void setRelationshipGuid(String relationshipGuid) { this.relationshipGuid = relationshipGuid; }

    @JsonProperty("relationshipStatus") public String getRelationshipStatus() { return this.relationshipStatus; }
    @JsonProperty("relationshipStatus") public void setRelationshipStatus(String relationshipStatus) { this.relationshipStatus = relationshipStatus; }

    @JsonProperty("relationshipAttributes") public Map<String, String> getRelationshipAttributes() { return this.relationshipAttributes; }
    @JsonProperty("relationshipAttributes") public void setRelationshipAttributes(Map<String, String> relationshipAttributes) { this.relationshipAttributes = relationshipAttributes; }

}
