/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.types;

import com.fasterxml.jackson.annotation.*;

import java.util.Date;

/**
 * Represents a single TypeDef header from Apache Atlas.
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=JsonTypeInfo.As.PROPERTY, property="category", visible=true, defaultImpl=TypeDefHeader.class)
@JsonSubTypes({
        @JsonSubTypes.Type(value = EnumTypeDef.class, name = "ENUM"),
        @JsonSubTypes.Type(value = StructTypeDef.class, name = "STRUCT"),
        @JsonSubTypes.Type(value = EntityTypeDef.class, name = "ENTITY"),
        @JsonSubTypes.Type(value = RelationshipTypeDef.class, name = "RELATIONSHIP"),
        @JsonSubTypes.Type(value = ClassificationTypeDef.class, name = "CLASSIFICATION")
})
@JsonIgnoreProperties(ignoreUnknown=true)
public class TypeDefHeader {

    /**
     * The GUID of the TypeDef in Apache Atlas.
     */
    private String guid;

    /**
     * The name of the TypeDef in Apache Atlas.
     */
    private String name;

    /**
     * The area of the model for the TypeDef in Apache Atlas.
     */
    private String serviceType;

    /**
     * The type of TypeDef in Apache Atlas.
     */
    private String category;

    private String createdBy;
    private String updatedBy;
    private Date createTime;
    private Date updateTime;
    private long version;
    private String description;
    private String typeVersion;

    @JsonProperty("guid") public String getGuid() { return this.guid; }
    @JsonProperty("guid") public void setGuid(String guid) { this.guid = guid; }

    @JsonProperty("name") public String getName() { return this.name; }
    @JsonProperty("name") public void setName(String name) { this.name = name; }

    @JsonProperty("serviceType") public String getServiceType() { return this.serviceType; }
    @JsonProperty("serviceType") public void setServiceType(String serviceType) { this.serviceType = serviceType; }

    @JsonProperty("category") public String getCategory() { return this.category; }
    @JsonProperty("category") public void setCategory(String category) { this.category = category; }

    @JsonProperty("createdBy") public String getCreatedBy() { return this.createdBy; }
    @JsonProperty("createdBy") public void setCreatedBy(String createdBy) { this.createdBy = createdBy; }

    @JsonProperty("updatedBy") public String getUpdatedBy() { return this.updatedBy; }
    @JsonProperty("updatedBy") public void setUpdatedBy(String updatedBy) { this.updatedBy = updatedBy; }

    @JsonProperty("createTime") public Date getCreateTime() { return this.createTime; }
    @JsonProperty("createTime") public void setCreateTime(Date createTime) { this.createTime = createTime; }

    @JsonProperty("updateTime") public Date getUpdateTime() { return this.updateTime; }
    @JsonProperty("updateTime") public void setUpdateTime(Date updateTime) { this.updateTime = updateTime; }

    @JsonProperty("version") public long getVersion() { return this.version; }
    @JsonProperty("version") public void setVersion(long version) { this.version = version; }

    @JsonProperty("description") public String getDescription() { return this.description; }
    @JsonProperty("description") public void setDescription(String description) { this.description = description; }

    @JsonProperty("typeVersion") public String getTypeVersion() { return this.typeVersion; }
    @JsonProperty("typeVersion") public void setTypeVersion(String typeVersion) { this.typeVersion = typeVersion; }

}
