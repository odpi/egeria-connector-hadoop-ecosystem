/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping.TypeDefMapping;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDef;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDefAttribute;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.TypeDefLink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Store of implemented TypeDefs for the repository.
 */
public class TypeDefStore {

    private static final Logger log = LoggerFactory.getLogger(TypeDefStore.class);

    private Map<String, TypeDef> omrsGuidToTypeDef;
    private Map<String, String> omrsNameToGuid;
    private Map<String, Map<String, TypeDefAttribute>> omrsGuidToAttributeMap;

    private Map<String, String> omrsNameToAtlasName;
    private Map<String, String> atlasNameToOmrsName;
    private Map<String, TypeDef> unimplementedTypeDefs;
    private Map<String, Map<String, String>> atlasNameToAttributeMap;
    private Map<String, Map<String, String>> omrsNameToAttributeMap;

    private ObjectMapper mapper;

    public TypeDefStore() {
        omrsGuidToTypeDef = new HashMap<>();
        omrsNameToGuid = new HashMap<>();
        omrsGuidToAttributeMap = new HashMap<>();
        omrsNameToAtlasName = new HashMap<>();
        atlasNameToOmrsName = new HashMap<>();
        unimplementedTypeDefs = new HashMap<>();
        omrsNameToAttributeMap = new HashMap<>();
        atlasNameToAttributeMap = new HashMap<>();
        mapper = new ObjectMapper();
        loadMappings();
    }

    /**
     * Loads TypeDef mappings defined through a resources file included in the .jar file.
     */
    private void loadMappings() {
        ClassPathResource mappingResource = new ClassPathResource("TypeDefMappings.json");
        try {
            InputStream stream = mappingResource.getInputStream();
            List<TypeDefMapping> mappings = mapper.readValue(stream, new TypeReference<List<TypeDefMapping>>(){});
            for (TypeDefMapping mapping : mappings) {
                String atlasName = mapping.getAtlasName();
                String omrsName = mapping.getOMRSName();
                omrsNameToAtlasName.put(omrsName, atlasName);
                atlasNameToOmrsName.put(atlasName, omrsName);
                List<TypeDefMapping> properties = mapping.getPropertyMappings();
                if (properties != null) {
                    Map<String, String> propertyMapOmrsToAtlas = new HashMap<>();
                    Map<String, String> propertyMapAtlasToOmrs = new HashMap<>();
                    for (TypeDefMapping property : properties) {
                        String atlasProperty = property.getAtlasName();
                        String omrsProperty = property.getOMRSName();
                        propertyMapOmrsToAtlas.put(omrsProperty, atlasProperty);
                        propertyMapAtlasToOmrs.put(atlasProperty, omrsProperty);
                    }
                    atlasNameToAttributeMap.put(atlasName, propertyMapAtlasToOmrs);
                    omrsNameToAttributeMap.put(omrsName, propertyMapOmrsToAtlas);
                }
            }
        } catch (IOException e) {
            log.error("Unable to load mapping file TypeDefMappings.json from jar file -- no mappings will exist.");
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
     * Retrieves a map from OMRS property name to Apache Atlas property name for the provided Apache Atlas TypeDef name,
     * or null if there are no mappings (or no properties).
     *
     * @param atlasName the name of the Apache Atlas TypeDef
     * @return {@code Map<String, String>}
     */
    public Map<String, String> getPropertyMappingsForAtlasTypeDef(String atlasName) {
        if (atlasNameToAttributeMap.containsKey(atlasName)) {
            return atlasNameToAttributeMap.get(atlasName);
        } else {
            return null;
        }
    }

    /**
     * Retrieves a map from OMRS property name to Apache Atlas property name for the provided OMRS TypeDef name, or null
     * if there are no mappings (or no properties).
     *
     * @param omrsName the name of the OMRS TypeDef
     * @return {@code Map<String, String>}
     */
    public Map<String, String> getPropertyMappingsForOMRSTypeDef(String omrsName) {
        if (omrsNameToAttributeMap.containsKey(omrsName)) {
            return omrsNameToAttributeMap.get(omrsName);
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
    public void addTypeDef(TypeDef typeDef) {
        String guid = typeDef.getGUID();
        omrsGuidToTypeDef.put(guid, typeDef);
        omrsNameToGuid.put(typeDef.getName(), guid);
        addAttributes(typeDef.getPropertiesDefinition(), guid);
    }

    /**
     * Adds the provided TypeDef to the list of those that are not implemented in the repository.
     * (Still needed for tracking and inheritance.)
     *
     * @param typeDef an unimplemented type definition
     */
    public void addUnimplementedTypeDef(TypeDef typeDef) {
        String guid = typeDef.getGUID();
        unimplementedTypeDefs.put(guid, typeDef);
        addAttributes(typeDef.getPropertiesDefinition(), guid);
    }

    /**
     * Adds a mapping between GUID of the OMRS TypeDef and a mapping of its attribute names to definitions.
     *
     * @param attributes the list of attribute definitions for the OMRS TypeDef
     * @param guid of the OMRS TypeDef
     */
    private void addAttributes(List<TypeDefAttribute> attributes, String guid) {
        if (!omrsGuidToAttributeMap.containsKey(guid)) {
            omrsGuidToAttributeMap.put(guid, new HashMap<>());
        }
        if (attributes != null) {
            for (TypeDefAttribute attribute : attributes) {
                omrsGuidToAttributeMap.get(guid).put(attribute.getAttributeName(), attribute);
            }
        }
    }

    /**
     * Retrieves an unimplemented TypeDef by its GUID.
     *
     * @param guid of the type definition
     * @return TypeDef
     */
    public TypeDef getUnimplementedTypeDefByGUID(String guid) {
        if (unimplementedTypeDefs.containsKey(guid)) {
            return unimplementedTypeDefs.get(guid);
        } else {
            if (log.isWarnEnabled()) { log.warn("Unable to find unimplemented OMRS TypeDef: {}", guid); }
            return null;
        }
    }

    /**
     * Retrieves an implemented TypeDef by its GUID.
     *
     * @param guid of the type definition
     * @return TypeDef
     */
    public TypeDef getTypeDefByGUID(String guid) {
        return getTypeDefByGUID(guid, true);
    }

    /**
     * Retrieves an implemented TypeDef by its GUID.
     *
     * @param guid of the type definition
     * @param warnIfNotFound whether to log a warning if GUID is not known (true) or not (false).
     * @return TypeDef
     */
    private TypeDef getTypeDefByGUID(String guid, boolean warnIfNotFound) {
        if (omrsGuidToTypeDef.containsKey(guid)) {
            return omrsGuidToTypeDef.get(guid);
        } else {
            if (warnIfNotFound) {
                if (log.isWarnEnabled()) { log.warn("Unable to find OMRS TypeDef by GUID: {}", guid); }
            }
            return null;
        }
    }

    /**
     * Retrieves an implemented TypeDef by its name.
     *
     * @param name of the type definition
     * @return TypeDef
     */
    public TypeDef getTypeDefByName(String name) {
        return getTypeDefByName(name, true);
    }

    /**
     * Retrieves an implemented TypeDef by its name.
     *
     * @param name of the type definition
     * @param warnIfNotFound whether to log a warning if name is not known (true) or not (false).
     * @return TypeDef
     */
    private TypeDef getTypeDefByName(String name, boolean warnIfNotFound) {
        if (omrsNameToGuid.containsKey(name)) {
            String guid = omrsNameToGuid.get(name);
            return getTypeDefByGUID(guid, warnIfNotFound);
        } else {
            if (warnIfNotFound) {
                if (log.isWarnEnabled()) { log.warn("Unable to find OMRS TypeDef by Name: {}", name); }
            }
            return null;
        }
    }

    /**
     * Retrieves a map from attribute name to attribute definition for all attributes of the specified type definition.
     *
     * @param guid of the type definition
     * @return {@code Map<String, TypeDefAttribute>}
     */
    private Map<String, TypeDefAttribute> getTypeDefAttributesByGUID(String guid) {
        if (omrsGuidToAttributeMap.containsKey(guid)) {
            return omrsGuidToAttributeMap.get(guid);
        } else {
            if (log.isWarnEnabled()) { log.warn("Unable to find attributes for OMRS TypeDef by GUID: {}", guid); }
            return null;
        }
    }

    /**
     * Retrieves a map from attribute name to attribute definition for all attributes of the specified type definition,
     * including all of its supertypes' attributes.
     *
     * @param guid of the type definition
     * @return {@code Map<String, TypeDefAttribute>}
     */
    public Map<String, TypeDefAttribute> getAllTypeDefAttributesForGUID(String guid) {
        Map<String, TypeDefAttribute> all = getTypeDefAttributesByGUID(guid);
        if (all != null) {
            TypeDef typeDef = getTypeDefByGUID(guid, false);
            if (typeDef == null) {
                typeDef = getUnimplementedTypeDefByGUID(guid);
            }
            TypeDefLink superType = typeDef.getSuperType();
            if (superType != null) {
                all.putAll(getAllTypeDefAttributesForGUID(superType.getGUID()));
            }
        }
        return all;
    }

    /**
     * Retrieves a map from attribute name to attribute definition for all attributes of the specified type definition,
     * including all of its supertypes' attributes.
     *
     * @param name of the type definition
     * @return {@code Map<String, TypeDefAttribute>}
     */
    public Map<String, TypeDefAttribute> getAllTypeDefAttributesForName(String name) {
        if (omrsNameToGuid.containsKey(name)) {
            String guid = omrsNameToGuid.get(name);
            return getAllTypeDefAttributesForGUID(guid);
        } else {
            if (log.isWarnEnabled()) { log.warn("Unable to find attributes for OMRS TypeDef by Name: {}", name); }
            return null;
        }
    }

    /**
     * Retrieves a listing of all of the implemented type definitions for this repository.
     *
     * @return {@code List<TypeDef>}
     */
    public List<TypeDef> getAllTypeDefs() {
        return new ArrayList<>(omrsGuidToTypeDef.values());
    }

}
