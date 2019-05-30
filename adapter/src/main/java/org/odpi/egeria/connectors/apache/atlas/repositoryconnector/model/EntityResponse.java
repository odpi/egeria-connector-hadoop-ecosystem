/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown=true)
@JsonTypeName("EntityResponse")
public class EntityResponse {

    private Map<String, EntityInstance> referredEntities;
    private EntityInstance entity;

    @JsonProperty("referredEntities") public Map<String, EntityInstance> getReferredEntities() { return this.referredEntities; }
    @JsonProperty("referredEntities") public void setReferredEntities(Map<String, EntityInstance> referredEntities) { this.referredEntities = referredEntities; }

    @JsonProperty("entity") public EntityInstance getEntity() { return this.entity; }
    @JsonProperty("entity") public void setEntity(EntityInstance entity) { this.entity = entity; }

}
