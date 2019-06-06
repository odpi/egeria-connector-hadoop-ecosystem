/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping.MappingFromFile;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

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
    private Map<String, EndpointMapping> atlasNameToEndpointMap;
    private Map<String, EndpointMapping> omrsNameToEndpointMap;

    private Set<String> unmappedTypes;

    private ObjectMapper mapper;

    public enum Endpoint {
        ONE, TWO, UNDEFINED
    }

    public TypeDefStore() {
        omrsGuidToTypeDef = new HashMap<>();
        omrsNameToGuid = new HashMap<>();
        omrsGuidToAttributeMap = new HashMap<>();
        omrsNameToAtlasName = new HashMap<>();
        atlasNameToOmrsName = new HashMap<>();
        unimplementedTypeDefs = new HashMap<>();
        omrsNameToAttributeMap = new HashMap<>();
        atlasNameToAttributeMap = new HashMap<>();
        atlasNameToEndpointMap = new HashMap<>();
        omrsNameToEndpointMap = new HashMap<>();
        unmappedTypes = new HashSet<>();
        mapper = new ObjectMapper();
        loadMappings();
        loadUnmapped();
    }

    /**
     * Loads TypeDef mappings defined through a resources file included in the .jar file.
     */
    private void loadMappings() {

        ClassPathResource mappingResource = new ClassPathResource("TypeDefMappings.json");

        try {

            InputStream stream = mappingResource.getInputStream();

            // Start with the basic mappings from type-to-type
            List<MappingFromFile> mappings = mapper.readValue(stream, new TypeReference<List<MappingFromFile>>(){});
            for (MappingFromFile mapping : mappings) {
                String atlasName = mapping.getAtlasName();
                String omrsName = mapping.getOMRSName();
                omrsNameToAtlasName.put(omrsName, atlasName);
                atlasNameToOmrsName.put(atlasName, omrsName);

                // Process any property-to-property mappings within the types
                List<MappingFromFile> properties = mapping.getPropertyMappings();
                if (properties != null) {
                    Map<String, String> propertyMapOmrsToAtlas = new HashMap<>();
                    Map<String, String> propertyMapAtlasToOmrs = new HashMap<>();
                    for (MappingFromFile property : properties) {
                        String atlasProperty = property.getAtlasName();
                        String omrsProperty = property.getOMRSName();
                        propertyMapOmrsToAtlas.put(omrsProperty, atlasProperty);
                        propertyMapAtlasToOmrs.put(atlasProperty, omrsProperty);
                    }
                    atlasNameToAttributeMap.put(atlasName, propertyMapAtlasToOmrs);
                    omrsNameToAttributeMap.put(omrsName, propertyMapOmrsToAtlas);
                }

                // Process any endpoint-to-endpoint mappings within the types (for relationships)
                List<MappingFromFile> endpoints = mapping.getEndpointMappings();
                if (endpoints != null) {
                    if (endpoints.size() != 2) {
                        log.warn("Skipping mapping as found other than exactly 2 endpoints defined for the relationship '{}': {}", atlasName, endpoints);
                    } else {
                        MappingFromFile endpoint1 = endpoints.get(0);
                        MappingFromFile endpoint2 = endpoints.get(1);
                        EndpointMapping endpointMapping = new EndpointMapping(
                                endpoint1.getAtlasName(),
                                endpoint1.getOMRSName(),
                                endpoint2.getAtlasName(),
                                endpoint2.getOMRSName()
                        );
                        atlasNameToEndpointMap.put(atlasName, endpointMapping);
                        omrsNameToEndpointMap.put(omrsName, endpointMapping);
                    }
                }

            }

        } catch (IOException e) {
            log.error("Unable to load mapping file TypeDefMappings.json from jar file -- no mappings will exist.");
        }
    }

    /**
     * Loads TypeDef mappings that should not be created, despite not being mapped (reserved for future mapping).
     */
    private void loadUnmapped() {

        ClassPathResource mappingResource = new ClassPathResource("Unmapped_OMRS.json");

        try {

            InputStream stream = mappingResource.getInputStream();

            // Start with the basic mappings from type-to-type
            List<String> omrsTypeNames = mapper.readValue(stream, new TypeReference<List<String>>(){});
            for (String omrsTypeName : omrsTypeNames) {
                unmappedTypes.add(omrsTypeName);
            }

        } catch (IOException e) {
            log.error("Unable to load reserved type file Unmapped_OMRS.json from jar file -- no types will be reserved for later mapping.");
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
     * Indicates whether the provided OMRS TypeDef is reserved for later mapping (and therefore should not be created).
     *
     * @param omrsName name of the OMRS TypeDef
     * @return boolean
     */
    public boolean isReserved(String omrsName) {
        return unmappedTypes.contains(omrsName);
    }

    /**
     * Retrieves a map from Apache Atlas property name to OMRS property name for the provided Apache Atlas TypeDef name,
     * or null if there are no mappings (or no properties).
     *
     * @param atlasName the name of the Apache Atlas TypeDef
     * @return {@code Map<String, String>}
     */
    public Map<String, String> getPropertyMappingsForAtlasTypeDef(String atlasName) {
        if (atlasNameToAttributeMap.containsKey(atlasName)) {
            return atlasNameToAttributeMap.get(atlasName);
        } else {
            return getPropertyMappingsForOMRSTypeDef(atlasName);
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
     * Retrieve the relationship endpoint mapped to the Apache Atlas details provided.
     *
     * @param atlasTypeName the name of the Apache Atlas type definition
     * @param atlasRelnAttrName the name of the Apache Atlas relationship attribute
     * @return Endpoint
     */
    public Endpoint getMappedEndpointFromAtlasName(String atlasTypeName, String atlasRelnAttrName) {
        EndpointMapping mapping = atlasNameToEndpointMap.get(atlasTypeName);
        if (mapping != null) {
            return mapping.getMatchingOmrsEndpoint(atlasRelnAttrName);
        } else {
            TypeDef typeDef = getTypeDefByName(atlasTypeName);
            if (typeDef != null && typeDef instanceof RelationshipDef) {
                RelationshipDef relationshipDef = (RelationshipDef) typeDef;
                RelationshipEndDef end1 = relationshipDef.getEndDef1();
                RelationshipEndDef end2 = relationshipDef.getEndDef2();
                if (end1.getAttributeName().equals(atlasRelnAttrName)) {
                    return Endpoint.ONE;
                } else if (end2.getAttributeName().equals(atlasRelnAttrName)) {
                    return Endpoint.TWO;
                } else {
                    return Endpoint.UNDEFINED;
                }
            } else {
                return Endpoint.UNDEFINED;
            }
        }
    }

    /**
     * Retrieves the Apache Atlas TypeDef name that is mapped to the provided OMRS TypeDef name, the same name if
     * there is a one-to-one mapping between Atlas and OMRS TypeDefs, or null if there is no mapping.
     *
     * @param omrsName the name of the OMRS TypeDef
     * @return String
     */
    public String getMappedAtlasTypeDefName(String omrsName) {
        if (isTypeDefMapped(omrsName)) {
            return omrsNameToAtlasName.get(omrsName);
        } else if (omrsNameToGuid.containsKey(omrsName)) {
            return omrsName;
        } else {
            return null;
        }
    }

    /**
     * Retrieves the OMRS TypeDef name that is mapped to the provided Apache Atlas TypeDef name, the same name if
     * there is a one-to-one mapping between Atlas and OMRS TypeDefs, or null if there is no mapping.
     *
     * @param atlasName the name of the Apache Atlas TypeDef
     * @return String
     */
    public String getMappedOMRSTypeDefName(String atlasName) {
        if (atlasNameToOmrsName.containsKey(atlasName)) {
            return atlasNameToOmrsName.get(atlasName);
        } else if (omrsNameToGuid.containsKey(atlasName)) {
            return atlasName;
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

    /**
     * For translating between relationship endpoints.
     */
    public final class EndpointMapping {

        private String atlas1;
        private String atlas2;
        private String omrs1;
        private String omrs2;

        protected EndpointMapping(String atlas1, String omrs1, String atlas2, String omrs2) {
            this.atlas1 = atlas1;
            this.atlas2 = atlas2;
            this.omrs1 = omrs1;
            this.omrs2 = omrs2;
        }

        /**
         * Retrieve the corresponding OMRS endpoint given an Apache Atlas endpoint attribute name.
         *
         * @param atlasEndpointName the Apache Atlas endpoint attribute name
         * @return Endpoint
         */
        public Endpoint getMatchingOmrsEndpoint(String atlasEndpointName) {
            if (atlasEndpointName != null) {
                if (atlasEndpointName.equals(atlas1)) {
                    return Endpoint.ONE;
                } else if (atlasEndpointName.equals(atlas2)) {
                    return Endpoint.TWO;
                }
            }
            return Endpoint.UNDEFINED;
        }

        /**
         * Retrieve the corresponding Apache Atlas endpoint given an OMRS endpoint attribute name.
         *
         * @param omrsEndpointName the OMRS endpoint attribute name
         * @return Endpoint
         */
        public Endpoint getMatchingAtlasEndpoint(String omrsEndpointName) {
            if (omrsEndpointName != null) {
                if (omrsEndpointName.equals(omrs1)) {
                    return Endpoint.ONE;
                } else if (omrsEndpointName.equals(omrs2)) {
                    return Endpoint.TWO;
                }
            }
            return Endpoint.UNDEFINED;
        }

    }

}
