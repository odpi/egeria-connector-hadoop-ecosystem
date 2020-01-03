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

    // About the OMRS TypeDefs themselves
    private Map<String, TypeDef> omrsGuidToTypeDef;
    private Map<String, String> omrsNameToGuid;
    private Map<String, Map<String, TypeDefAttribute>> omrsGuidToAttributeMap;
    private Map<String, TypeDef> unimplementedTypeDefs;

    // Mapping details
    private Map<String, Map<String, String>> omrsNameToAtlasNamesByPrefix;
    private Map<String, Map<String, String>> atlasNameToOmrsNamesByPrefix;
    private Map<String, Map<String, Map<String, String>>> omrsNameToAttributeMapByPrefix;
    private Map<String, Map<String, Map<String, String>>> atlasNameToAttributeMapByPrefix;
    private Map<String, Map<String, EndpointMapping>> omrsNameToEndpointMapByPrefix;
    private Map<String, Map<String, EndpointMapping>> atlasNameToEndpointMapByPrefix;

    private Set<String> unmappedTypes;

    private ObjectMapper mapper;

    public enum Endpoint {
        ONE, TWO, UNDEFINED
    }

    public TypeDefStore() {
        omrsGuidToTypeDef = new HashMap<>();
        omrsNameToGuid = new HashMap<>();
        omrsGuidToAttributeMap = new HashMap<>();
        omrsNameToAtlasNamesByPrefix = new HashMap<>();
        atlasNameToOmrsNamesByPrefix = new HashMap<>();
        unimplementedTypeDefs = new HashMap<>();
        omrsNameToAttributeMapByPrefix = new HashMap<>();
        atlasNameToAttributeMapByPrefix = new HashMap<>();
        omrsNameToEndpointMapByPrefix = new HashMap<>();
        atlasNameToEndpointMapByPrefix = new HashMap<>();
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
                String prefix = mapping.getPrefix();

                if (!omrsNameToAtlasNamesByPrefix.containsKey(omrsName)) {
                    omrsNameToAtlasNamesByPrefix.put(omrsName, new HashMap<>());
                }
                omrsNameToAtlasNamesByPrefix.get(omrsName).put(prefix, atlasName);
                if (!atlasNameToOmrsNamesByPrefix.containsKey(atlasName)) {
                    atlasNameToOmrsNamesByPrefix.put(atlasName, new HashMap<>());
                }
                atlasNameToOmrsNamesByPrefix.get(atlasName).put(prefix, omrsName);

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
                    if (!omrsNameToAttributeMapByPrefix.containsKey(omrsName)) {
                        omrsNameToAttributeMapByPrefix.put(omrsName, new HashMap<>());
                    }
                    omrsNameToAttributeMapByPrefix.get(omrsName).put(prefix, propertyMapOmrsToAtlas);
                    if (!atlasNameToAttributeMapByPrefix.containsKey(atlasName)) {
                        atlasNameToAttributeMapByPrefix.put(atlasName, new HashMap<>());
                    }
                    atlasNameToAttributeMapByPrefix.get(atlasName).put(prefix, propertyMapAtlasToOmrs);
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
                                atlasName,
                                omrsName,
                                endpoint1.getAtlasName(),
                                endpoint1.getOMRSName(),
                                endpoint1.getPrefix(),
                                endpoint2.getAtlasName(),
                                endpoint2.getOMRSName(),
                                endpoint2.getPrefix()
                        );
                        if (!omrsNameToEndpointMapByPrefix.containsKey(omrsName)) {
                            omrsNameToEndpointMapByPrefix.put(omrsName, new HashMap<>());
                        }
                        omrsNameToEndpointMapByPrefix.get(omrsName).put(prefix, endpointMapping);
                        if (!atlasNameToEndpointMapByPrefix.containsKey(atlasName)) {
                            atlasNameToEndpointMapByPrefix.put(atlasName, new HashMap<>());
                        }
                        atlasNameToEndpointMapByPrefix.get(atlasName).put(prefix, endpointMapping);
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
            unmappedTypes.addAll(omrsTypeNames);

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
        return omrsNameToAtlasNamesByPrefix.containsKey(omrsName);
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
     * @param prefix the prefix (if any) when mappings to multiple types exist
     * @return {@code Map<String, String>}
     */
    public Map<String, String> getPropertyMappingsForAtlasTypeDef(String atlasName, String prefix) {
        if (atlasNameToAttributeMapByPrefix.containsKey(atlasName)) {
            if (atlasNameToAttributeMapByPrefix.get(atlasName).containsKey(prefix)) {
                return atlasNameToAttributeMapByPrefix.get(atlasName).get(prefix);
            } else {
                return getPropertyMappingsForOMRSTypeDef(atlasName, prefix);
            }
        } else {
            return getPropertyMappingsForOMRSTypeDef(atlasName, prefix);
        }
    }

    /**
     * Retrieves a map from OMRS property name to Apache Atlas property name for the provided OMRS TypeDef name, or null
     * if there are no mappings (or no properties).
     *
     * @param omrsName the name of the OMRS TypeDef
     * @param prefix the prefix (if any) when mappings to multiple types exist
     * @return {@code Map<String, String>}
     */
    public Map<String, String> getPropertyMappingsForOMRSTypeDef(String omrsName, String prefix) {
        if (omrsNameToAttributeMapByPrefix.containsKey(omrsName)) {
            return omrsNameToAttributeMapByPrefix.get(omrsName).getOrDefault(prefix, null);
        } else {
            return null;
        }
    }

    /**
     * Retrieve the relationship endpoint mapped to the Apache Atlas details provided.
     *
     * @param atlasTypeName the name of the Apache Atlas type definition
     * @param atlasRelnAttrName the name of the Apache Atlas relationship attribute
     * @param entityPrefix the prefix used for the entity, if it is a generated entity (null if not generated)
     * @return Endpoint
     */
    public Endpoint getMappedEndpointFromAtlasName(String atlasTypeName, String atlasRelnAttrName, String entityPrefix) {
        if (atlasNameToEndpointMapByPrefix.containsKey(atlasTypeName)) {
            EndpointMapping mapping = atlasNameToEndpointMapByPrefix.get(atlasTypeName).getOrDefault(entityPrefix, null);
            if (mapping != null) {
                return mapping.getMatchingOmrsEndpoint(atlasRelnAttrName, entityPrefix);
            }
        }
        // If we fall through, look for a TypeDef that we created as part of OMRS registration (in which cases no
        // prefixes or generation should be involved, so we can skip those checks)
        TypeDef typeDef = getTypeDefByName(atlasTypeName);
        if (typeDef instanceof RelationshipDef) {
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

    /**
     * Retrieve the relationship endpoint mapping from the Apache Atlas details provided.
     *
     * @param atlasTypeName the name of the Apache Atlas type definition
     * @param entityPrefix the prefix used for the entity, if it is a generated entity (null if not generated)
     * @return EndpointMapping
     */
    public EndpointMapping getEndpointMappingFromAtlasName(String atlasTypeName, String entityPrefix) {
        if (atlasNameToEndpointMapByPrefix.containsKey(atlasTypeName)) {
            return atlasNameToEndpointMapByPrefix.get(atlasTypeName).getOrDefault(entityPrefix, null);
        } else {
            return null;
        }
    }

    /**
     * Retrieve all endpoint mappings (relationships) that are mapped for the provided Apache Atlas type.
     *
     * @param atlasTypeName the name of the Apache Atlas type definition
     * @return {@code Map<String, EndpointMapping>}
     */
    public Map<String, EndpointMapping> getAllEndpointMappingsFromAtlasName(String atlasTypeName) {
        return atlasNameToEndpointMapByPrefix.getOrDefault(atlasTypeName, null);
    }

    /**
     * Retrieves all of the Apache Atlas TypeDef names that are mapped to the provided OMRS TypeDef name, or null
     * if there is no mapping. The map returned will be keyed by prefix, and values will be the mapped Atlas TypeDef
     * name for that prefix.
     *
     * @param omrsName the name of the OMRS TypeDef
     * @return {@code Map<String, String>}
     */
    public Map<String, String> getAllMappedAtlasTypeDefNames(String omrsName) {
        if (isTypeDefMapped(omrsName)) {
            return omrsNameToAtlasNamesByPrefix.get(omrsName);
        } else if (omrsNameToGuid.containsKey(omrsName)) {
            Map<String, String> map = new HashMap<>();
            map.put(null, omrsName);
            return map;
        } else {
            return null;
        }
    }

    /**
     * Retrieves the Apache Atlas TypeDef name that is mapped to the provided OMRS TypeDef name, the same name if
     * there is a one-to-one mapping between Atlas and OMRS TypeDefs, or null if there is no mapping.
     *
     * @param omrsName the name of the OMRS TypeDef
     * @param prefix the prefix (if any) when mappings to multiple types exist
     * @return String
     */
    public String getMappedAtlasTypeDefName(String omrsName, String prefix) {
        if (isTypeDefMapped(omrsName)) {
            return omrsNameToAtlasNamesByPrefix.get(omrsName).getOrDefault(prefix, null);
        } else if (omrsNameToGuid.containsKey(omrsName)) {
            return omrsName;
        } else {
            return null;
        }
    }

    /**
     * Retrieves all of the OMRS TypeDef names that are mapped to the provided OMRS TypeDef name, or null if there is
     * no mapping. The map returned will be keyed by prefix, and values will be the mapped OMRS TypeDef name for that
     * prefix.
     *
     * @param atlasName the name of the Apache Atlas TypeDef
     * @return {@code Map<String, String>}
     */
    public Map<String, String> getAllMappedOMRSTypeDefNames(String atlasName) {
        return atlasNameToOmrsNamesByPrefix.getOrDefault(atlasName, null);
    }

    /**
     * Retrieves the OMRS TypeDef name that is mapped to the provided Apache Atlas TypeDef name, the same name if
     * there is a one-to-one mapping between Atlas and OMRS TypeDefs, or null if there is no mapping.
     *
     * @param atlasName the name of the Apache Atlas TypeDef
     * @param prefix the prefix (if any) when mappings to multiple types exist
     * @return String
     */
    public String getMappedOMRSTypeDefName(String atlasName, String prefix) {
        if (atlasNameToOmrsNamesByPrefix.containsKey(atlasName)) {
            return atlasNameToOmrsNamesByPrefix.get(atlasName).getOrDefault(prefix, null);
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
        addAttributes(typeDef.getPropertiesDefinition(), guid, typeDef.getName());
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
        addAttributes(typeDef.getPropertiesDefinition(), guid, typeDef.getName());
    }

    /**
     * Adds a mapping between GUID of the OMRS TypeDef and a mapping of its attribute names to definitions.
     *
     * @param attributes the list of attribute definitions for the OMRS TypeDef
     * @param guid of the OMRS TypeDef
     * @param name of the OMRS TypeDef
     */
    private void addAttributes(List<TypeDefAttribute> attributes, String guid, String name) {
        if (!omrsGuidToAttributeMap.containsKey(guid)) {
            omrsGuidToAttributeMap.put(guid, new HashMap<>());
        }
        if (attributes != null) {
            Map<String, String> oneToOne = new HashMap<>();
            for (TypeDefAttribute attribute : attributes) {
                String propertyName = attribute.getAttributeName();
                omrsGuidToAttributeMap.get(guid).put(propertyName, attribute);
                oneToOne.put(propertyName, propertyName);
            }
            if (!omrsNameToAttributeMapByPrefix.containsKey(name)) {
                // If no mapping was loaded for this OMRS type definition, add one-to-one mappings
                omrsNameToAttributeMapByPrefix.put(name, new HashMap<>());
                omrsNameToAttributeMapByPrefix.get(name).put(null, oneToOne);
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
    public TypeDef getTypeDefByGUID(String guid, boolean warnIfNotFound) {
        if (omrsGuidToTypeDef.containsKey(guid)) {
            return omrsGuidToTypeDef.get(guid);
        } else {
            if (warnIfNotFound && log.isWarnEnabled()) {
                log.warn("Unable to find OMRS TypeDef by GUID: {}", guid);
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
            if (warnIfNotFound && log.isWarnEnabled()) {
                log.warn("Unable to find OMRS TypeDef by Name: {}", name);
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
    private Map<String, TypeDefAttribute> getAllTypeDefAttributesForGUID(String guid) {
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

        private String atlasRelationshipTypeName;
        private String omrsRelationshipTypeName;
        private String atlas1;
        private String atlas2;
        private String omrs1;
        private String omrs2;
        private String prefix1;
        private String prefix2;

        EndpointMapping(String atlasRelationshipTypeName,
                        String omrsRelationshipTypeName,
                        String atlas1,
                        String omrs1,
                        String prefix1,
                        String atlas2,
                        String omrs2,
                        String prefix2) {
            this.atlasRelationshipTypeName = atlasRelationshipTypeName;
            this.omrsRelationshipTypeName = omrsRelationshipTypeName;
            this.atlas1 = atlas1;
            this.atlas2 = atlas2;
            this.omrs1 = omrs1;
            this.omrs2 = omrs2;
            this.prefix1 = prefix1;
            this.prefix2 = prefix2;
        }

        /**
         * Retrieve the corresponding OMRS endpoint given an Apache Atlas endpoint attribute name.
         *
         * @param atlasEndpointName the Apache Atlas endpoint attribute name
         * @param entityPrefix the prefix used for the entity, if it is a generated entity (null if not generated)
         * @return Endpoint
         */
        public Endpoint getMatchingOmrsEndpoint(String atlasEndpointName, String entityPrefix) {
            if (atlasEndpointName != null) {
                // If there is a named Atlas endpoint, check that the prefixes are the same before choosing an endpoint
                if (atlasEndpointName.equals(atlas1) && samePrefixes(prefix1, entityPrefix)) {
                    return Endpoint.ONE;
                } else if (atlasEndpointName.equals(atlas2) && samePrefixes(prefix2, entityPrefix)) {
                    return Endpoint.TWO;
                }
            } else {
                // If there is no named Atlas endpoint, check that recorded endpoint is also null and prefixes also match
                if (atlas1 == null && samePrefixes(prefix1, entityPrefix)) {
                    return Endpoint.ONE;
                } else if (atlas2 == null && samePrefixes(prefix2, entityPrefix)) {
                    return Endpoint.TWO;
                }
            }
            return Endpoint.UNDEFINED;
        }

        /**
         * Retrieve the corresponding Apache Atlas endpoint given an OMRS endpoint attribute name.
         *
         * @param omrsEndpointName the OMRS endpoint attribute name
         * @param entityPrefix the prefix for the entity (if any)
         * @return Endpoint
         */
        public Endpoint getMatchingAtlasEndpoint(String omrsEndpointName, String entityPrefix) {
            if (omrsEndpointName != null) {
                if (omrsEndpointName.equals(omrs1) && samePrefixes(prefix1, entityPrefix)) {
                    return Endpoint.ONE;
                } else if (omrsEndpointName.equals(omrs2) && samePrefixes(prefix2, entityPrefix)) {
                    return Endpoint.TWO;
                }
            } else {
                if (omrs1 == null && samePrefixes(prefix1, entityPrefix)) {
                    return Endpoint.ONE;
                } else if (omrs2 == null && samePrefixes(prefix2, entityPrefix)) {
                    return Endpoint.TWO;
                }
            }
            return Endpoint.UNDEFINED;
        }

        public String getPrefixOne() { return prefix1; }
        public String getPrefixTwo() { return prefix2; }
        public String getAtlasRelationshipTypeName() { return atlasRelationshipTypeName; }
        public String getOmrsRelationshipTypeName() { return omrsRelationshipTypeName; }

        /**
         * Indicates whether the two prefixes are the same (true) or not (false).
         *
         * @param cand1 first prefix to compare
         * @param cand2 second prefix to compare
         * @return boolean
         */
        private boolean samePrefixes(String cand1, String cand2) {
            return (cand1 == null && cand2 == null)
                    || (cand1 != null && cand2 != null && cand1.equals(cand2));
        }

    }

}
