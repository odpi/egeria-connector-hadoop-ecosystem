/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.instances;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown=true)
@JsonTypeName("ClassificationAssignment")
public class ClassificationAssignment {

    private String typeName;
    private Map<String, String> attributes;
    private String entityGuid;
    private String entityStatus;
    private boolean propagate;
    private List<ValidityPeriod> validityPeriods;
    private boolean removePropagationsOnEntityDelete;

    @JsonProperty("typeName") public String getTypeName() { return this.typeName; }
    @JsonProperty("typeName") public void setTypeName(String typeName) { this.typeName = typeName; }

    @JsonProperty("attributes") public Map<String, String> getAttributes() { return this.attributes; }
    @JsonProperty("attributes") public void setAttributes(Map<String, String> attributes) { this.attributes = attributes; }

    @JsonProperty("entityGuid") public String getEntityGuid() { return this.entityGuid; }
    @JsonProperty("entityGuid") public void setEntityGuid(String entityGuid) { this.entityGuid = entityGuid; }

    @JsonProperty("entityStatus") public String getEntityStatus() { return this.entityStatus; }
    @JsonProperty("entityStatus") public void setEntityStatus(String entityStatus) { this.entityStatus = entityStatus; }

    @JsonProperty("propagate") public boolean isPropagate() { return this.propagate; }
    @JsonProperty("propagate") public void setPropagate(boolean propagate) { this.propagate = propagate; }

    @JsonProperty("validityPeriods") public List<ValidityPeriod> getValidityPeriods() { return this.validityPeriods; }
    @JsonProperty("validityPeriods") public void setValidityPeriods(List<ValidityPeriod> validityPeriods) { this.validityPeriods = validityPeriods; }

    @JsonProperty("removePropagationsOnEntityDelete") public boolean isRemovePropagationsOnEntityDelete() { return this.removePropagationsOnEntityDelete; }
    @JsonProperty("removePropagationsOnEntityDelete") public void setRemovePropagationsOnEntityDelete(boolean removePropagationsOnEntityDelete) { this.removePropagationsOnEntityDelete = removePropagationsOnEntityDelete; }

}
