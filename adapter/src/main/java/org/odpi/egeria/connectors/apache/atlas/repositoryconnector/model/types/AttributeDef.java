/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.types;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonIgnoreProperties(ignoreUnknown=true)
@JsonTypeName("AttributeDef")
public class AttributeDef {

    private String name;
    private String typeName;
    private boolean isOptional;
    private String cardinality;
    private long valuesMinCount;
    private long valuesMaxCount;
    private boolean isUnique;
    private boolean isIndexable;
    private boolean includeInNotification;

    private String defaultValue;
    private String description;

    @JsonProperty("name") public String getName() { return this.name; }
    @JsonProperty("name") public void setName(String name) { this.name = name; }

    @JsonProperty("typeName") public String getTypeName() { return this.typeName; }
    @JsonProperty("typeName") public void setTypeName(String typeName) { this.typeName = typeName; }

    @JsonProperty("isOptional") public boolean isOptional() { return this.isOptional; }
    @JsonProperty("isOptional") public void setOptional(boolean isOptional) { this.isOptional = isOptional; }

    @JsonProperty("cardinality") public String getCardinality() { return this.cardinality; }
    @JsonProperty("cardinality") public void setCardinality(String cardinality) { this.cardinality = cardinality; }

    @JsonProperty("valuesMinCount") public long getValuesMinCount() { return this.valuesMinCount; }
    @JsonProperty("valuesMinCount") public void setValuesMinCount(long valuesMinCount) { this.valuesMinCount = valuesMinCount; }

    @JsonProperty("valuesMaxCount") public long getValuesMaxCount() { return this.valuesMaxCount; }
    @JsonProperty("valuesMaxCount") public void setValuesMaxCount(long valuesMaxCount) { this.valuesMaxCount = valuesMaxCount; }

    @JsonProperty("isUnique") public boolean isUnique() { return this.isUnique; }
    @JsonProperty("isUnique") public void setUnique(boolean isUnique) { this.isUnique = isUnique; }

    @JsonProperty("isIndexable") public boolean isIndexable() { return this.isIndexable; }
    @JsonProperty("isIndexable") public void setIndexable(boolean isIndexable) { this.isIndexable = isIndexable; }

    @JsonProperty("includeInNotification") public boolean includeInNotification() { return this.includeInNotification; }
    @JsonProperty("includeInNotification") public void setIncludeInNotification(boolean includeInNotification) { this.includeInNotification = includeInNotification; }

    @JsonProperty("defaultValue") public String getDefaultValue() { return this.defaultValue; }
    @JsonProperty("defaultValue") public void setDefaultValue(String defaultValue) { this.defaultValue = defaultValue; }

    @JsonProperty("description") public String getDescription() { return this.description; }
    @JsonProperty("description") public void setDescription(String description) { this.description = description; }

}
