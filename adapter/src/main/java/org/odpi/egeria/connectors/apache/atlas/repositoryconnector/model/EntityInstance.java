/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Date;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown=true)
@JsonTypeName("EntityInstance")
public class EntityInstance {

    private String typeName;
    private Map<String, String> attributes;
    private String guid;
    private String status;
    private String createdBy;
    private String updatedBy;
    private Date createTime;
    private Date updateTime;
    private long version;
    // TODO: relationshipAttributes

    @JsonProperty("typeName") public String getTypeName() { return this.typeName; }
    @JsonProperty("typeName") public void setTypeName(String typeName) { this.typeName = typeName; }

    @JsonProperty("attributes") public Map<String, String> getAttributes() { return this.attributes; }
    @JsonProperty("attributes") public void setAttributes(Map<String, String> attributes) { this.attributes = attributes; }

    @JsonProperty("guid") public String getGuid() { return this.guid; }
    @JsonProperty("guid") public void setGuid(String guid) { this.guid = guid; }

    @JsonProperty("status") public String getStatus() { return this.status; }
    @JsonProperty("status") public void setStatus(String status) { this.status = status; }

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

}
