/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model;

import java.util.Objects;

/**
 * Captures the meaning and translation of the various components of an Atlas GUID.
 *
 * This is necessary to cover those scenarios where an instance is generated for Apache Atlas, because what is a single
 * instance in Apache Atlas is actually represented as multiple instances in OMRS. As a result, the GUIDs for these
 * generated entities will require some prefixing to allow them to be properly handled.
 */
public class AtlasGuid {

    private static final String GENERATED_TYPE_POSTFIX = "!";

    private String atlasGuid;
    private String generatedPrefix;

    /**
     * Create a new Apache Atlas GUID that has a prefix (for an OMRS instance type that does not actually exist in Apache
     * Atlas, but is generated from another instance type in Atlas)
     *
     * @param atlasGuid the GUID of the Apache Atlas asset
     * @param prefix the prefix to use to uniquely identify this generated instance's GUID
     */
    public AtlasGuid(String atlasGuid, String prefix) {
        this.generatedPrefix = prefix;
        this.atlasGuid = atlasGuid;
    }

    /**
     * Turn this Apache Atlas GUID into a unique String representation of the GUID.
     *
     * The string representation will be something like the following:
     * {@literal database_schema@5e74232d-92df-4b81-a401-b100dbfea73a:RDBST!6662c0f2.ee6a64fe.o1h6eveh1.gbvjvq0.ols3j6.0oadmdn8gknhjvmojr3pt}
     *
     * @return String
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (generatedPrefix != null) {
            sb.append(generatedPrefix);
            sb.append(GENERATED_TYPE_POSTFIX);
        }
        sb.append(atlasGuid);
        return sb.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof AtlasGuid)) return false;
        AtlasGuid that = (AtlasGuid) obj;
        return Objects.equals(getAtlasGuid(), that.getAtlasGuid()) &&
                Objects.equals(getGeneratedPrefix(), that.getGeneratedPrefix());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return Objects.hash(getAtlasGuid(), getGeneratedPrefix());
    }

    /**
     * Attempt to create a new Apache Atlas GUID from the provided GUID.
     *
     * @param guidToConvert the Apache Atlas GUID representation of the provided GUID, or null if it does not appear to
     *                      be a valid Apache Atlas GUID
     * @return AtlasGuid
     */
    public static AtlasGuid fromGuid(String guidToConvert) {

        if (guidToConvert == null) {
            return null;
        }

        String atlasGuid = guidToConvert;
        String generatedPrefix = null;
        int indexOfGeneratedPostfix = guidToConvert.indexOf(GENERATED_TYPE_POSTFIX);
        if (indexOfGeneratedPostfix > 0) {
            generatedPrefix = guidToConvert.substring(0, indexOfGeneratedPostfix);
            atlasGuid = guidToConvert.substring(indexOfGeneratedPostfix + 1);
        }
        return new AtlasGuid(atlasGuid, generatedPrefix);

    }

    /**
     * Retrieve the generated prefix component of this Apache Atlas GUID, if it is for a generated instance (or null if
     * the instance is not generated).
     *
     * @return String
     */
    public String getGeneratedPrefix() { return generatedPrefix; }

    /**
     * Indicates whether this Apache Atlas GUID represents a generated instance (true) or not (false).
     *
     * @return boolean
     */
    public boolean isGeneratedInstanceGuid() { return generatedPrefix != null; }

    /**
     * Retrieve the Apache Atlas GUID.
     *
     * @return String
     */
    public String getAtlasGuid() { return atlasGuid; }

}
