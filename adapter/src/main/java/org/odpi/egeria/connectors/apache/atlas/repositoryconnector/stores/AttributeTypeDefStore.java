/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping.MappingFromFile;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.AttributeTypeDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Store of implemented AttributeTypeDefs for the repository.
 */
public class AttributeTypeDefStore {

    private static final Logger log = LoggerFactory.getLogger(AttributeTypeDefStore.class);

    private Map<String, String> omrsNameToAtlasName;
    private Map<String, String> atlasNameToOmrsName;
    private Map<String, AttributeTypeDef> omrsGuidToTypeDef;
    private Map<String, String> omrsNameToGuid;

    private Map<String, AttributeTypeDef> unimplementedTypeDefs;
    private Map<String, Map<String, String>> atlasNameToElementMap;
    private Map<String, Map<String, String>> omrsNameToElementMap;

    private ObjectMapper mapper;

    public AttributeTypeDefStore() {
        this.omrsNameToAtlasName = new HashMap<>();
        this.atlasNameToOmrsName = new HashMap<>();
        this.omrsGuidToTypeDef = new HashMap<>();
        this.omrsNameToGuid = new HashMap<>();
        this.unimplementedTypeDefs = new HashMap<>();
        this.atlasNameToElementMap = new HashMap<>();
        this.omrsNameToElementMap = new HashMap<>();
        this.mapper = new ObjectMapper();
        loadMappings();
    }

    /**
     * Loads TypeDef mappings defined through a resources file included in the .jar file.
     */
    private void loadMappings() {
        ClassPathResource mappingResource = new ClassPathResource("EnumDefMappings.json");
        try {
            InputStream stream = mappingResource.getInputStream();
            List<MappingFromFile> mappings = mapper.readValue(stream, new TypeReference<List<MappingFromFile>>(){});
            for (MappingFromFile mapping : mappings) {
                String atlasName = mapping.getAtlasName();
                String omrsName = mapping.getOMRSName();
                omrsNameToAtlasName.put(omrsName, atlasName);
                atlasNameToOmrsName.put(atlasName, omrsName);
                List<MappingFromFile> elements = mapping.getPropertyMappings();
                if (elements != null) {
                    Map<String, String> elementMapOmrsToAtlas = new HashMap<>();
                    Map<String, String> elementMapAtlasToOmrs = new HashMap<>();
                    for (MappingFromFile element : elements) {
                        String atlasElement = element.getAtlasName();
                        String omrsElement = element.getOMRSName();
                        elementMapOmrsToAtlas.put(omrsElement, atlasElement);
                        elementMapAtlasToOmrs.put(atlasElement, omrsElement);
                    }
                    atlasNameToElementMap.put(atlasName, elementMapAtlasToOmrs);
                    omrsNameToElementMap.put(omrsName, elementMapOmrsToAtlas);
                }
            }
        } catch (IOException e) {
            log.error("Unable to load mapping file EnumDefMappings.json from jar file -- no mappings will exist.");
        }
    }

    /**
     * Indicates whether the provided OMRS TypeDef is mapped to an Apache Atlas TypeDef.
     *
     * @param omrsName name of the OMRS TypeDef
     * @return boolean
     */
    public boolean isTypeDefMapped(String omrsName) {
        return omrsNameToAtlasName.containsKey(omrsName);
    }

    /**
     * Retrieves a map from Apache Atlas enum value to OMRS enum value for the provided Apache Atlas TypeDef name,
     * or null if there are no mappings (or no enum elements).
     *
     * @param atlasName the name of the Apache Atlas TypeDef
     * @return {@code Map<String, String>}
     */
    public Map<String, String> getElementMappingsForAtlasTypeDef(String atlasName) {
        if (atlasNameToElementMap.containsKey(atlasName)) {
            return atlasNameToElementMap.get(atlasName);
        } else {
            return null;
        }
    }

    /**
     * Retrieves a map from OMRS enum value to Apache Atlas enum value for the provided OMRS TypeDef name, or null
     * if there are no mappings (or no enum elements).
     *
     * @param omrsName the name of the OMRS TypeDef
     * @return {@code Map<String, String>}
     */
    public Map<String, String> getElementMappingsForOMRSTypeDef(String omrsName) {
        if (omrsNameToElementMap.containsKey(omrsName)) {
            return omrsNameToElementMap.get(omrsName);
        } else {
            return null;
        }
    }

    /**
     * Retrieves the Apache Atlas TypeDef name that is mapped to the provided OMRS TypeDef name, or null if there is
     * no mapping.
     *
     * @param omrsName the name of the OMRS TypeDef
     * @return String
     */
    public String getMappedAtlasTypeDefName(String omrsName) {
        if (isTypeDefMapped(omrsName)) {
            return omrsNameToAtlasName.get(omrsName);
        } else {
            return null;
        }
    }

    /**
     * Retrieves the OMRS TypeDef name that is mapped to the provided Apache Atlas TypeDef name, or null if there is
     * no mapping.
     *
     * @param atlasName the name of the Apache Atlas TypeDef
     * @return String
     */
    public String getMappedOMRSTypeDefName(String atlasName) {
        if (atlasNameToOmrsName.containsKey(atlasName)) {
            return atlasNameToOmrsName.get(atlasName);
        } else {
            return null;
        }
    }

    /**
     * Adds the provided TypeDef to the list of those that are implemented in the repository.
     *
     * @param typeDef an implemented type definition
     */
    public void addTypeDef(AttributeTypeDef typeDef) {
        String guid = typeDef.getGUID();
        String name = typeDef.getName();
        omrsGuidToTypeDef.put(guid, typeDef);
        omrsNameToGuid.put(name, guid);
        if (!omrsNameToAtlasName.containsKey(name)) {
            omrsNameToAtlasName.put(name, name);
            atlasNameToOmrsName.put(name, name);
        }
    }

    /**
     * Adds the provided TypeDef to the list of those that are not implemented in the repository.
     * (Still needed for tracking and inheritance.)
     *
     * @param typeDef an unimplemented type definition
     */
    public void addUnimplementedTypeDef(AttributeTypeDef typeDef) {
        String guid = typeDef.getGUID();
        unimplementedTypeDefs.put(guid, typeDef);
    }

}
