/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector;

import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping.AttributeMapping;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping.ClassificationDefMapping;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping.EntityMapping;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping.EnumDefMapping;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores.AttributeTypeDefStore;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores.TypeDefStore;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.OMRSMetadataCollectionBase;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.MatchCriteria;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.SequencingOrder;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.*;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryValidator;
import org.odpi.openmetadata.repositoryservices.ffdc.OMRSErrorCode;
import org.odpi.openmetadata.repositoryservices.ffdc.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ApacheAtlasOMRSMetadataCollection extends OMRSMetadataCollectionBase {

    private static final Logger log = LoggerFactory.getLogger(ApacheAtlasOMRSMetadataCollection.class);

    private ApacheAtlasOMRSRepositoryConnector atlasRepositoryConnector;
    private TypeDefStore typeDefStore;
    private AttributeTypeDefStore attributeTypeDefStore;
    private Set<InstanceStatus> availableStates;

    /**
     * @param parentConnector      connector that this metadata collection supports.
     *                             The connector has the information to call the metadata repository.
     * @param repositoryName       name of this repository.
     * @param repositoryHelper     helper that provides methods to repository connectors and repository event mappers
     *                             to build valid type definitions (TypeDefs), entities and relationships.
     * @param repositoryValidator  validator class for checking open metadata repository objects and parameters
     * @param metadataCollectionId unique identifier for the repository
     */
    public ApacheAtlasOMRSMetadataCollection(ApacheAtlasOMRSRepositoryConnector parentConnector,
                                             String repositoryName,
                                             OMRSRepositoryHelper repositoryHelper,
                                             OMRSRepositoryValidator repositoryValidator,
                                             String metadataCollectionId) {
        super(parentConnector, repositoryName, repositoryHelper, repositoryValidator, metadataCollectionId);
        parentConnector.setRepositoryName(repositoryName);
        this.atlasRepositoryConnector = parentConnector;
        this.typeDefStore = new TypeDefStore();
        this.attributeTypeDefStore = new AttributeTypeDefStore();
        this.availableStates = new HashSet<>();
        availableStates.add(InstanceStatus.ACTIVE);
        availableStates.add(InstanceStatus.DELETED);
    }

    /**
     * Returns the list of different types of metadata organized into two groups.  The first are the
     * attribute type definitions (AttributeTypeDefs).  These provide types for properties in full
     * type definitions.  Full type definitions (TypeDefs) describe types for entities, relationships
     * and classifications.
     *
     * @param userId unique identifier for requesting user.
     * @return TypeDefGalleryResponse List of different categories of type definitions.
     * @throws RepositoryErrorException there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException the userId is not permitted to perform this operation.
     */
    @Override
    public TypeDefGallery getAllTypes(String   userId) throws RepositoryErrorException,
            UserNotAuthorizedException {

        final String                       methodName = "getAllTypes";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);

        /*
         * Perform operation
         */
        TypeDefGallery typeDefGallery = new TypeDefGallery();
        List<TypeDef> typeDefs = typeDefStore.getAllTypeDefs();
        if (log.isDebugEnabled()) { log.debug("Retrieved {} implemented TypeDefs for this repository.", typeDefs.size()); }
        typeDefGallery.setTypeDefs(typeDefs);

        List<AttributeTypeDef> attributeTypeDefs = attributeTypeDefStore.getAllAttributeTypeDefs();
        if (log.isDebugEnabled()) { log.debug("Retrieved {} implemented AttributeTypeDefs for this repository.", attributeTypeDefs.size()); }
        typeDefGallery.setAttributeTypeDefs(attributeTypeDefs);

        return typeDefGallery;

    }

    /**
     * Returns all of the TypeDefs for a specific category.
     *
     * @param userId unique identifier for requesting user.
     * @param category enum value for the category of TypeDef to return.
     * @return TypeDefs list.
     * @throws InvalidParameterException the TypeDefCategory is null.
     * @throws RepositoryErrorException there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException the userId is not permitted to perform this operation.
     */
    @Override
    public List<TypeDef> findTypeDefsByCategory(String          userId,
                                                TypeDefCategory category) throws InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException {

        final String methodName            = "findTypeDefsByCategory";
        final String categoryParameterName = "category";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeDefCategory(repositoryName, categoryParameterName, category, methodName);

        List<TypeDef> typeDefs = new ArrayList<>();
        for (TypeDef candidate : typeDefStore.getAllTypeDefs()) {
            if (candidate.getCategory().equals(category)) {
                typeDefs.add(candidate);
            }
        }
        return typeDefs;

    }

    /**
     * Returns all of the AttributeTypeDefs for a specific category.
     *
     * @param userId unique identifier for requesting user.
     * @param category enum value for the category of an AttributeTypeDef to return.
     * @return TypeDefs list.
     * @throws InvalidParameterException the TypeDefCategory is null.
     * @throws RepositoryErrorException there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException the userId is not permitted to perform this operation.
     */
    @Override
    public List<AttributeTypeDef> findAttributeTypeDefsByCategory(String                   userId,
                                                                  AttributeTypeDefCategory category) throws InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException {

        final String methodName            = "findAttributeTypeDefsByCategory";
        final String categoryParameterName = "category";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateAttributeTypeDefCategory(repositoryName, categoryParameterName, category, methodName);

        List<AttributeTypeDef> typeDefs = new ArrayList<>();
        for (AttributeTypeDef candidate : attributeTypeDefStore.getAllAttributeTypeDefs()) {
            if (candidate.getCategory().equals(category)) {
                typeDefs.add(candidate);
            }
        }
        return typeDefs;

    }

    /**
     * Return the TypeDefs that have the properties matching the supplied match criteria.
     *
     * @param userId unique identifier for requesting user.
     * @param matchCriteria TypeDefProperties containing a list of property names.
     * @return TypeDefs list.
     * @throws InvalidParameterException the matchCriteria is null.
     * @throws RepositoryErrorException there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException the userId is not permitted to perform this operation.
     */
    @Override
    public List<TypeDef> findTypeDefsByProperty(String            userId,
                                                TypeDefProperties matchCriteria) throws InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException {

        final String  methodName                 = "findTypeDefsByProperty";
        final String  matchCriteriaParameterName = "matchCriteria";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateMatchCriteria(repositoryName, matchCriteriaParameterName, matchCriteria, methodName);

        /*
         * Perform operation
         */
        List<TypeDef> typeDefs = typeDefStore.getAllTypeDefs();
        List<TypeDef> results = new ArrayList<>();
        if (matchCriteria != null) {
            Map<String, Object> properties = matchCriteria.getTypeDefProperties();
            for (TypeDef candidate : typeDefs) {
                List<TypeDefAttribute> candidateProperties = candidate.getPropertiesDefinition();
                for (TypeDefAttribute candidateAttribute : candidateProperties) {
                    String candidateName = candidateAttribute.getAttributeName();
                    if (properties.containsKey(candidateName)) {
                        results.add(candidate);
                    }
                }
            }
            results = typeDefs;
        }

        return results;

    }

    /**
     * Return the types that are linked to the elements from the specified standard.
     *
     * @param userId unique identifier for requesting user.
     * @param standard name of the standard; null means any.
     * @param organization name of the organization; null means any.
     * @param identifier identifier of the element in the standard; null means any.
     * @return TypeDefs list each entry in the list contains a typedef.  This is is a structure
     * describing the TypeDef's category and properties.
     * @throws InvalidParameterException all attributes of the external id are null.
     * @throws RepositoryErrorException there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException the userId is not permitted to perform this operation.
     */
    @Override
    public List<TypeDef> findTypesByExternalID(String    userId,
                                               String    standard,
                                               String    organization,
                                               String    identifier) throws InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException {

        final String                       methodName = "findTypesByExternalID";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateExternalId(repositoryName, standard, organization, identifier, methodName);

        /*
         * Perform operation
         */
        List<TypeDef> typeDefs = typeDefStore.getAllTypeDefs();
        List<TypeDef> results;
        if (standard == null && organization == null && identifier == null) {
            results = typeDefs;
        } else {
            results = new ArrayList<>();
            for (TypeDef typeDef : typeDefs) {
                List<ExternalStandardMapping> externalStandardMappings = typeDef.getExternalStandardMappings();
                for (ExternalStandardMapping externalStandardMapping : externalStandardMappings) {
                    String candidateStandard = externalStandardMapping.getStandardName();
                    String candidateOrg = externalStandardMapping.getStandardOrganization();
                    String candidateId = externalStandardMapping.getStandardTypeName();
                    if ( (standard == null || standard.equals(candidateStandard))
                            && (organization == null || organization.equals(candidateOrg))
                            && (identifier == null || identifier.equals(candidateId))) {
                        results.add(typeDef);
                    }
                }
            }
        }

        return results;

    }

    /**
     * Return the TypeDefs that match the search criteria.
     *
     * @param userId unique identifier for requesting user.
     * @param searchCriteria String search criteria.
     * @return TypeDefs list where each entry in the list contains a typedef.  This is is a structure
     * describing the TypeDef's category and properties.
     * @throws InvalidParameterException the searchCriteria is null.
     * @throws RepositoryErrorException there is a problem communicating with the metadata repository.
     * @throws UserNotAuthorizedException the userId is not permitted to perform this operation.
     */
    @Override
    public List<TypeDef> searchForTypeDefs(String userId,
                                           String searchCriteria) throws InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException {

        final String methodName                  = "searchForTypeDefs";
        final String searchCriteriaParameterName = "searchCriteria";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateSearchCriteria(repositoryName, searchCriteriaParameterName, searchCriteria, methodName);

        /*
         * Perform operation
         */
        List<TypeDef> typeDefs = new ArrayList<>();
        for (TypeDef candidate : typeDefStore.getAllTypeDefs()) {
            if (candidate.getName().matches(searchCriteria)) {
                typeDefs.add(candidate);
            }
        }
        return typeDefs;

    }

    /**
     * Return the TypeDef identified by the GUID.
     *
     * @param userId unique identifier for requesting user.
     * @param guid String unique id of the TypeDef.
     * @return TypeDef structure describing its category and properties.
     * @throws InvalidParameterException the guid is null.
     * @throws RepositoryErrorException there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws TypeDefNotKnownException The requested TypeDef is not known in the metadata collection.
     * @throws UserNotAuthorizedException the userId is not permitted to perform this operation.
     */
    @Override
    public TypeDef getTypeDefByGUID(String    userId,
                                    String    guid) throws InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotKnownException,
            UserNotAuthorizedException {

        final String methodName        = "getTypeDefByGUID";
        final String guidParameterName = "guid";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

        TypeDef found = typeDefStore.getTypeDefByGUID(guid);

        if (found == null) {
            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_ID_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(
                    guid,
                    guidParameterName,
                    methodName,
                    repositoryName);
            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        return found;

    }

    /**
     * Return the AttributeTypeDef identified by the GUID.
     *
     * @param userId unique identifier for requesting user.
     * @param guid String unique id of the TypeDef
     * @return TypeDef structure describing its category and properties.
     * @throws InvalidParameterException the guid is null.
     * @throws RepositoryErrorException there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws TypeDefNotKnownException The requested TypeDef is not known in the metadata collection.
     * @throws UserNotAuthorizedException the userId is not permitted to perform this operation.
     */
    @Override
    public  AttributeTypeDef getAttributeTypeDefByGUID(String    userId,
                                                       String    guid) throws InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotKnownException,
            UserNotAuthorizedException {

        final String methodName        = "getAttributeTypeDefByGUID";
        final String guidParameterName = "guid";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

        AttributeTypeDef found = attributeTypeDefStore.getAttributeTypeDefByGUID(guid);
        if (found == null) {
            OMRSErrorCode errorCode = OMRSErrorCode.ATTRIBUTE_TYPEDEF_ID_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(
                    "unknown",
                    guid,
                    methodName,
                    repositoryName);
            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        return found;

    }

    /**
     * Return the TypeDef identified by the unique name.
     *
     * @param userId unique identifier for requesting user.
     * @param name String name of the TypeDef.
     * @return TypeDef structure describing its category and properties.
     * @throws InvalidParameterException the name is null.
     * @throws RepositoryErrorException there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws TypeDefNotKnownException the requested TypeDef is not found in the metadata collection.
     * @throws UserNotAuthorizedException the userId is not permitted to perform this operation.
     */
    @Override
    public TypeDef getTypeDefByName(String    userId,
                                    String    name) throws InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotKnownException,
            UserNotAuthorizedException {

        final String  methodName = "getTypeDefByName";
        final String  nameParameterName = "name";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeName(repositoryName, nameParameterName, name, methodName);

        /*
         * Perform operation
         */
        TypeDef found = typeDefStore.getTypeDefByName(name);

        if (found == null) {
            OMRSErrorCode errorCode = OMRSErrorCode.TYPEDEF_NAME_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(
                    name,
                    methodName,
                    repositoryName);
            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }

        return found;

    }

    /**
     * Return the AttributeTypeDef identified by the unique name.
     *
     * @param userId unique identifier for requesting user.
     * @param name String name of the TypeDef.
     * @return TypeDef structure describing its category and properties.
     * @throws InvalidParameterException the name is null.
     * @throws RepositoryErrorException there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws TypeDefNotKnownException the requested TypeDef is not found in the metadata collection.
     * @throws UserNotAuthorizedException the userId is not permitted to perform this operation.
     */
    @Override
    public  AttributeTypeDef getAttributeTypeDefByName(String    userId,
                                                       String    name) throws InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotKnownException,
            UserNotAuthorizedException {

        final String  methodName = "getAttributeTypeDefByName";
        final String  nameParameterName = "name";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeName(repositoryName, nameParameterName, name, methodName);

        AttributeTypeDef found = attributeTypeDefStore.getAttributeTypeDefByName(name);
        if (found == null) {
            OMRSErrorCode errorCode = OMRSErrorCode.ATTRIBUTE_TYPEDEF_NAME_NOT_KNOWN;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(
                    name,
                    methodName,
                    repositoryName);
            throw new TypeDefNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        return found;
    }

    /**
     * Create a definition of a new TypeDef.
     *
     * @param userId unique identifier for requesting user.
     * @param newTypeDef TypeDef structure describing the new TypeDef.
     * @throws InvalidParameterException the new TypeDef is null.
     * @throws RepositoryErrorException there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws TypeDefNotSupportedException the repository is not able to support this TypeDef.
     * @throws TypeDefKnownException the TypeDef is already stored in the repository.
     * @throws TypeDefConflictException the new TypeDef conflicts with an existing TypeDef.
     * @throws InvalidTypeDefException the new TypeDef has invalid contents.
     * @throws FunctionNotSupportedException the repository does not support this call.
     * @throws UserNotAuthorizedException the userId is not permitted to perform this operation.
     */
    @Override
    public void addTypeDef(String  userId,
                           TypeDef newTypeDef) throws InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotSupportedException,
            TypeDefKnownException,
            TypeDefConflictException,
            InvalidTypeDefException,
            FunctionNotSupportedException,
            UserNotAuthorizedException {

        final String  methodName = "addTypeDef";
        final String  typeDefParameterName = "newTypeDef";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeDef(repositoryName, typeDefParameterName, newTypeDef, methodName);
        repositoryValidator.validateUnknownTypeDef(repositoryName, typeDefParameterName, newTypeDef, methodName);

        TypeDefCategory typeDefCategory = newTypeDef.getCategory();
        String omrsTypeDefName = newTypeDef.getName();
        if (log.isDebugEnabled()) { log.debug("Looking for mapping for {} of type {}", omrsTypeDefName, typeDefCategory.getName()); }

        if (typeDefStore.isTypeDefMapped(omrsTypeDefName)) {

            // If it is a mapped TypeDef, add it to our store
            typeDefStore.addTypeDef(newTypeDef);

        } else if (newTypeDef.getCategory().equals(TypeDefCategory.CLASSIFICATION_DEF)) {

            if (atlasRepositoryConnector.typeDefExistsByName(omrsTypeDefName)) {
                // If the TypeDef already exists in Atlas, add it to our store
                // TODO: should really still verify it, in case TypeDef changes
                typeDefStore.addTypeDef(newTypeDef);
            } else {
                // Otherwise, if it is a Classification, we'll add it to Atlas itself
                ClassificationDefMapping.addClassificationToAtlas(
                        (ClassificationDef) newTypeDef,
                        typeDefStore,
                        attributeTypeDefStore,
                        atlasRepositoryConnector
                );
            }

        } else {
            // Otherwise, we'll drop it as unimplemented
            typeDefStore.addUnimplementedTypeDef(newTypeDef);
            throw new TypeDefNotSupportedException(
                    404,
                    ApacheAtlasOMRSMetadataCollection.class.getName(),
                    methodName,
                    omrsTypeDefName + " is not supported.",
                    "",
                    "Request support through Egeria GitHub issue."
            );
        }

    }

    /**
     * Create a definition of a new AttributeTypeDef.
     *
     * @param userId unique identifier for requesting user.
     * @param newAttributeTypeDef TypeDef structure describing the new TypeDef.
     * @throws InvalidParameterException the new TypeDef is null.
     * @throws RepositoryErrorException there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws TypeDefNotSupportedException the repository is not able to support this TypeDef.
     * @throws TypeDefKnownException the TypeDef is already stored in the repository.
     * @throws TypeDefConflictException the new TypeDef conflicts with an existing TypeDef.
     * @throws InvalidTypeDefException the new TypeDef has invalid contents.
     * @throws FunctionNotSupportedException the repository does not support this call.
     * @throws UserNotAuthorizedException the userId is not permitted to perform this operation.
     */
    @Override
    public  void addAttributeTypeDef(String             userId,
                                     AttributeTypeDef   newAttributeTypeDef) throws InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotSupportedException,
            TypeDefKnownException,
            TypeDefConflictException,
            InvalidTypeDefException,
            FunctionNotSupportedException,
            UserNotAuthorizedException {

        final String  methodName           = "addAttributeTypeDef";
        final String  typeDefParameterName = "newAttributeTypeDef";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateAttributeTypeDef(repositoryName, typeDefParameterName, newAttributeTypeDef, methodName);
        repositoryValidator.validateUnknownAttributeTypeDef(repositoryName, typeDefParameterName, newAttributeTypeDef, methodName);

        // Note this is only implemented for Enums, support for other types is indicated directly
        // in the verifyAttributeTypeDef method
        AttributeTypeDefCategory attributeTypeDefCategory = newAttributeTypeDef.getCategory();
        String omrsTypeDefName = newAttributeTypeDef.getName();
        if (log.isDebugEnabled()) { log.debug("Looking for mapping for {} of type {}", omrsTypeDefName, attributeTypeDefCategory.getName()); }

        if (attributeTypeDefStore.isTypeDefMapped(omrsTypeDefName)) {

            // If it is a mapped TypeDef, add it to our store
            attributeTypeDefStore.addTypeDef(newAttributeTypeDef);

        } else if (newAttributeTypeDef.getCategory().equals(AttributeTypeDefCategory.ENUM_DEF)) {

            if (atlasRepositoryConnector.typeDefExistsByName(omrsTypeDefName)) {
                // If the TypeDef already exists in Atlas, add it to our store
                // TODO: should really still verify it, in case TypeDef changes
                attributeTypeDefStore.addTypeDef(newAttributeTypeDef);
            } else {
                // Otherwise, if it is a Classification, we'll add it to Atlas itself
                EnumDefMapping.addEnumToAtlas(
                        (EnumDef) newAttributeTypeDef,
                        attributeTypeDefStore,
                        atlasRepositoryConnector
                );
            }

        } else {
            // Otherwise, we'll drop it as unimplemented
            attributeTypeDefStore.addUnimplementedTypeDef(newAttributeTypeDef);
            throw new TypeDefNotSupportedException(
                    404,
                    ApacheAtlasOMRSMetadataCollection.class.getName(),
                    methodName,
                    omrsTypeDefName + " is not supported.",
                    "",
                    "Request support through Egeria GitHub issue."
            );
        }

    }

    /**
     * Verify that a definition of a TypeDef is either new or matches the definition already stored.
     *
     * @param userId unique identifier for requesting user.
     * @param typeDef TypeDef structure describing the TypeDef to test.
     * @return boolean true means the TypeDef matches the local definition; false means the TypeDef is not known.
     * @throws InvalidParameterException the TypeDef is null.
     * @throws RepositoryErrorException there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws TypeDefNotSupportedException the repository is not able to support this TypeDef.
     * @throws TypeDefConflictException the new TypeDef conflicts with an existing TypeDef.
     * @throws InvalidTypeDefException the new TypeDef has invalid contents.
     * @throws UserNotAuthorizedException the userId is not permitted to perform this operation.
     */
    @Override
    public boolean verifyTypeDef(String  userId,
                                 TypeDef typeDef) throws InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotSupportedException,
            TypeDefConflictException,
            InvalidTypeDefException,
            UserNotAuthorizedException {

        final String  methodName           = "verifyTypeDef";
        final String  typeDefParameterName = "typeDef";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);
        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateTypeDef(repositoryName, typeDefParameterName, typeDef, methodName);

        String guid = typeDef.getGUID();

        // If we know the TypeDef is unimplemented, immediately throw an exception stating as much
        if (typeDefStore.getUnimplementedTypeDefByGUID(guid) != null) {
            throw new TypeDefNotSupportedException(
                    404,
                    ApacheAtlasOMRSMetadataCollection.class.getName(),
                    methodName,
                    typeDef.getName() + " is not supported.",
                    "",
                    "Request support through Egeria GitHub issue.");
        } else if (typeDefStore.getTypeDefByGUID(guid) != null) {

            // TODO: Validate that we support all of the valid InstanceStatus settings before deciding whether we fully-support the TypeDef or not
            boolean bVerified = true;
            List<String> issues = new ArrayList<>();

            // Validate that we support all of the possible properties before deciding whether we
            // fully-support the TypeDef or not
            String omrsTypeDefName = typeDef.getName();
            List<TypeDefAttribute> properties = typeDef.getPropertiesDefinition();
            if (properties != null) {
                Map<String, String> mappedProperties = typeDefStore.getPropertyMappingsForOMRSTypeDef(omrsTypeDefName);
                for (TypeDefAttribute typeDefAttribute : properties) {
                    String omrsPropertyName = typeDefAttribute.getAttributeName();
                    if (!mappedProperties.containsKey(omrsPropertyName)) {
                        bVerified = false;
                        issues.add("property '" + omrsPropertyName + "' is not mapped");
                    }
                }
            }

            // If we were unable to verify everything, throw exception indicating it is not a supported TypeDef
            if (!bVerified) {
                throw new TypeDefNotSupportedException(
                        404,
                        ApacheAtlasOMRSMetadataCollection.class.getName(),
                        methodName,
                        typeDef.getName() + " is not supported: " + String.join(", ", issues),
                        "",
                        "Request support through Egeria GitHub issue.");
            }

            return bVerified;

        } else {
            // It is completely unknown to us, so go ahead and try to addTypeDef
            return false;
        }

    }

    /**
     * Verify that a definition of an AttributeTypeDef is either new or matches the definition already stored.
     *
     * @param userId unique identifier for requesting user.
     * @param attributeTypeDef TypeDef structure describing the TypeDef to test.
     * @return boolean where true means the TypeDef matches the local definition where false means the TypeDef is not known.
     * @throws InvalidParameterException the TypeDef is null.
     * @throws RepositoryErrorException there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws TypeDefNotSupportedException the repository is not able to support this TypeDef.
     * @throws TypeDefConflictException the new TypeDef conflicts with an existing TypeDef.
     * @throws InvalidTypeDefException the new TypeDef has invalid contents.
     * @throws UserNotAuthorizedException the userId is not permitted to perform this operation.
     */
    @Override
    public boolean verifyAttributeTypeDef(String            userId,
                                           AttributeTypeDef  attributeTypeDef) throws InvalidParameterException,
            RepositoryErrorException,
            TypeDefNotSupportedException,
            TypeDefConflictException,
            InvalidTypeDefException,
            UserNotAuthorizedException {

        final String  methodName           = "verifyAttributeTypeDef";
        final String  typeDefParameterName = "attributeTypeDef";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateAttributeTypeDef(repositoryName, typeDefParameterName, attributeTypeDef, methodName);

        boolean bImplemented;
        switch (attributeTypeDef.getCategory()) {
            case PRIMITIVE:
            case COLLECTION:
                bImplemented = true;
                break;
            case ENUM_DEF:
                bImplemented = attributeTypeDefStore.isTypeDefMapped(attributeTypeDef.getName());
                break;
            default:
                bImplemented = false;
                break;
        }

        return bImplemented;

    }

    /**
     * Returns the entity if the entity is stored in the metadata collection, otherwise null.
     *
     * @param userId unique identifier for requesting user.
     * @param guid String unique identifier for the entity
     * @return the entity details if the entity is found in the metadata collection; otherwise return null
     * @throws InvalidParameterException the guid is null.
     * @throws RepositoryErrorException there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws UserNotAuthorizedException the userId is not permitted to perform this operation.
     */
    @Override
    public EntityDetail isEntityKnown(String     userId,
                                      String     guid) throws InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException {

        final String  methodName = "isEntityKnown";
        final String  guidParameterName = "guid";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

        /*
         * Perform operation
         */
        EntityDetail detail = null;
        try {
            detail = getEntityDetail(userId, guid);
        } catch (EntityNotKnownException | EntityProxyOnlyException e) {
            if (log.isInfoEnabled()) { log.info("Entity {} not known to the repository, or only a proxy.", guid, e); }
        }
        return detail;
    }

    /**
     * Return the header and classifications for a specific entity.
     *
     * @param userId unique identifier for requesting user.
     * @param guid String unique identifier for the entity.
     * @return EntitySummary structure
     * @throws InvalidParameterException the guid is null.
     * @throws RepositoryErrorException there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws EntityNotKnownException the requested entity instance is not known in the metadata collection.
     * @throws UserNotAuthorizedException the userId is not permitted to perform this operation.
     */
    @Override
    public EntitySummary getEntitySummary(String     userId,
                                          String     guid) throws InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            UserNotAuthorizedException {

        final String  methodName        = "getEntitySummary";
        final String  guidParameterName = "guid";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

        /*
         * Perform operation
         */
        AtlasEntity.AtlasEntityWithExtInfo entity = this.atlasRepositoryConnector.getEntityByGUID(guid);
        if (entity == null) {
            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(guid,
                    methodName,
                    repositoryName);
            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        EntityMapping mapping = new EntityMapping(atlasRepositoryConnector, typeDefStore, attributeTypeDefStore, entity, userId);
        return mapping.getEntitySummary();

    }

    /**
     * Return the header, classifications and properties of a specific entity.
     *
     * @param userId unique identifier for requesting user.
     * @param guid String unique identifier for the entity.
     * @return EntityDetail structure.
     * @throws InvalidParameterException the guid is null.
     * @throws RepositoryErrorException there is a problem communicating with the metadata repository where
     *                                 the metadata collection is stored.
     * @throws EntityNotKnownException the requested entity instance is not known in the metadata collection.
     * @throws EntityProxyOnlyException the requested entity instance is only a proxy in the metadata collection.
     * @throws UserNotAuthorizedException the userId is not permitted to perform this operation.
     */
    @Override
    public EntityDetail getEntityDetail(String userId,
                                        String guid) throws InvalidParameterException,
            RepositoryErrorException,
            EntityNotKnownException,
            EntityProxyOnlyException,
            UserNotAuthorizedException {

        final String  methodName        = "getEntityDetail";
        final String  guidParameterName = "guid";

        /*
         * Validate parameters
         */
        this.validateRepositoryConnector(methodName);
        parentConnector.validateRepositoryIsActive(methodName);

        repositoryValidator.validateUserId(repositoryName, userId, methodName);
        repositoryValidator.validateGUID(repositoryName, guidParameterName, guid, methodName);

        /*
         * Perform operation
         */
        AtlasEntity.AtlasEntityWithExtInfo entity = this.atlasRepositoryConnector.getEntityByGUID(guid);
        if (entity == null) {
            OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
            String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(guid,
                    methodName,
                    repositoryName);
            throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        }
        EntityMapping mapping = new EntityMapping(atlasRepositoryConnector, typeDefStore, attributeTypeDefStore, entity, userId);
        return mapping.getEntityDetail();

    }

    /**
     * Return the relationships for a specific entity.
     *
     * @param userId unique identifier for requesting user.
     * @param entityGUID String unique identifier for the entity.
     * @param relationshipTypeGUID String GUID of the the type of relationship required (null for all).
     * @param fromRelationshipElement the starting element number of the relationships to return.
     *                                This is used when retrieving elements
     *                                beyond the first page of results. Zero means start from the first element.
     * @param limitResultsByStatus Not implemented for Apache Atlas -- will only retrieve ACTIVE entities.
     * @param asOfTime Must be null (history not implemented for Apache Atlas).
     * @param sequencingProperty String name of the property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder Enum defining how the results should be ordered.
     * @param pageSize -- the maximum number of result classifications that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @return Relationships list.  Null means no relationships associated with the entity.
     * @throws InvalidParameterException a parameter is invalid or null.
     * @throws TypeErrorException the type guid passed on the request is not known by the
     *                              metadata collection.
     * @throws RepositoryErrorException there is a problem communicating with the metadata repository where
     *                                  the metadata collection is stored.
     * @throws EntityNotKnownException the requested entity instance is not known in the metadata collection.
     * @throws PropertyErrorException the sequencing property is not valid for the attached classifications.
     * @throws PagingErrorException the paging/sequencing parameters are set up incorrectly.
     * @throws FunctionNotSupportedException the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException the userId is not permitted to perform this operation.
     */
    @Override
    public List<Relationship> getRelationshipsForEntity(String                     userId,
                                                        String                     entityGUID,
                                                        String                     relationshipTypeGUID,
                                                        int                        fromRelationshipElement,
                                                        List<InstanceStatus>       limitResultsByStatus,
                                                        Date                       asOfTime,
                                                        String                     sequencingProperty,
                                                        SequencingOrder            sequencingOrder,
                                                        int                        pageSize) throws InvalidParameterException,
            TypeErrorException,
            RepositoryErrorException,
            EntityNotKnownException,
            PropertyErrorException,
            PagingErrorException,
            FunctionNotSupportedException,
            UserNotAuthorizedException {

        final String  methodName = "getRelationshipsForEntity";
        getRelationshipsForEntityParameterValidation(
                userId,
                entityGUID,
                relationshipTypeGUID,
                fromRelationshipElement,
                limitResultsByStatus,
                asOfTime,
                sequencingProperty,
                sequencingOrder,
                pageSize
        );

        List<Relationship> alRelationships = null;

        // Immediately throw unimplemented exception if trying to retrieve historical view or sequence by property
        if (asOfTime != null) {
            OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                    this.getClass().getName(),
                    repositoryName);
            throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        } else if (limitResultsByStatus == null
                || (limitResultsByStatus.size() == 1 && limitResultsByStatus.contains(InstanceStatus.ACTIVE))) {

            // Otherwise, only bother searching if we are after ACTIVE (or "all") entities -- non-ACTIVE means we
            // will just return an empty list

            // 1. retrieve entity from Apache Atlas by GUID (including its relationships)
            AtlasEntity.AtlasEntityWithExtInfo asset = atlasRepositoryConnector.getEntityByGUID(entityGUID, false, false);

            // Ensure the entity actually exists (if not, throw error to that effect)
            if (asset == null) {
                OMRSErrorCode errorCode = OMRSErrorCode.ENTITY_NOT_KNOWN;
                String        errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(entityGUID,
                        methodName,
                        repositoryName);
                throw new EntityNotKnownException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            } else {

                EntityMapping entityMap = new EntityMapping(
                        atlasRepositoryConnector,
                        typeDefStore,
                        attributeTypeDefStore,
                        asset,
                        userId
                );

                // 2. Apply the mapping to the object, and retrieve the resulting relationships
                alRelationships = entityMap.getRelationships(
                        relationshipTypeGUID,
                        fromRelationshipElement,
                        sequencingProperty,
                        sequencingOrder,
                        pageSize
                );

            }

        }

        return alRelationships;

    }

    /**
     * Return a list of entities that match the supplied properties according to the match criteria.  The results
     * can be returned over many pages.
     *
     * @param userId unique identifier for requesting user.
     * @param entityTypeGUID String unique identifier for the entity type of interest (null means any entity type).
     * @param matchProperties Optional list of entity properties to match (contains wildcards).
     * @param matchCriteria Enum defining how the match properties should be matched to the entities in the repository.
     * @param fromEntityElement the starting element number of the entities to return.
     *                                This is used when retrieving elements
     *                                beyond the first page of results. Zero means start from the first element.
     * @param limitResultsByStatus By default, entities in all statuses are returned.  However, it is possible
     *                             to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                             status values.
     * @param limitResultsByClassification List of classifications that must be present on all returned entities.
     * @param asOfTime Requests a historical query of the entity (not implemented for Atlas).
     * @param sequencingProperty String name of the entity property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder Enum defining how the results should be ordered.
     * @param pageSize the maximum number of result entities that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @return a list of entities matching the supplied criteria; null means no matching entities in the metadata
     * collection.
     * @throws InvalidParameterException a parameter is invalid or null.
     * @throws TypeErrorException the type guid passed on the request is not known by the
     *                              metadata collection.
     * @throws RepositoryErrorException there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws PropertyErrorException the properties specified are not valid for any of the requested types of
     *                                  entity.
     * @throws PagingErrorException the paging/sequencing parameters are set up incorrectly.
     * @throws FunctionNotSupportedException the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException the userId is not permitted to perform this operation.
     */
    @Override
    public  List<EntityDetail> findEntitiesByProperty(String                    userId,
                                                      String                    entityTypeGUID,
                                                      InstanceProperties        matchProperties,
                                                      MatchCriteria             matchCriteria,
                                                      int                       fromEntityElement,
                                                      List<InstanceStatus>      limitResultsByStatus,
                                                      List<String>              limitResultsByClassification,
                                                      Date                      asOfTime,
                                                      String                    sequencingProperty,
                                                      SequencingOrder           sequencingOrder,
                                                      int                       pageSize) throws InvalidParameterException,
            TypeErrorException,
            RepositoryErrorException,
            PropertyErrorException,
            PagingErrorException,
            FunctionNotSupportedException,
            UserNotAuthorizedException {

        final String methodName = "findEntitiesByProperty";
        findEntitiesByPropertyParameterValidation(
                userId,
                entityTypeGUID,
                matchProperties,
                matchCriteria,
                fromEntityElement,
                limitResultsByStatus,
                limitResultsByClassification,
                asOfTime,
                sequencingProperty,
                sequencingOrder,
                pageSize
        );

        AtlasSearchResult results;

        // Immediately throw unimplemented exception if trying to retrieve historical view
        if (asOfTime != null) {
            OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                    this.getClass().getName(),
                    repositoryName);
            throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        } else if (sequencingOrder != null || (limitResultsByClassification != null && limitResultsByClassification.size() > 1)) {

            results = buildAndRunDSLSearch(
                    methodName,
                    entityTypeGUID,
                    limitResultsByClassification,
                    matchProperties,
                    matchCriteria,
                    fromEntityElement,
                    limitResultsByStatus,
                    sequencingProperty,
                    sequencingOrder,
                    pageSize
            );

        } else {

            // Only take the first classification for a basic search (if there were multiple, should be handled above
            // by DSL query)
            results = buildAndRunBasicSearch(
                    methodName,
                    entityTypeGUID,
                    (limitResultsByClassification == null ? null : limitResultsByClassification.get(0)),
                    matchProperties,
                    matchCriteria,
                    fromEntityElement,
                    limitResultsByStatus,
                    pageSize
            );

        }

        List<EntityDetail> entityDetails = null;
        if (results != null) {
            entityDetails = getEntityDetailsFromAtlasResults(results.getEntities(), userId);
        }
        return (entityDetails == null || entityDetails.isEmpty()) ? null : entityDetails;

    }

    /**
     * Return a list of entities that have the requested type of classifications attached.
     *
     * @param userId unique identifier for requesting user.
     * @param entityTypeGUID unique identifier for the type of entity requested.  Null means any type of entity
     *                       (but could be slow so not recommended.
     * @param classificationName name of the classification, note a null is not valid.
     * @param matchClassificationProperties Optional list of entity properties to match (contains wildcards).
     * @param matchCriteria Enum defining how the match properties should be matched to the classifications in the repository.
     * @param fromEntityElement the starting element number of the entities to return.
     *                                This is used when retrieving elements
     *                                beyond the first page of results. Zero means start from the first element.
     * @param limitResultsByStatus By default, entities in all statuses are returned.  However, it is possible
     *                             to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                             status values.
     * @param asOfTime Requests a historical query of the entity.  Null means return the present values.
     * @param sequencingProperty String name of the entity property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder Enum defining how the results should be ordered.
     * @param pageSize the maximum number of result entities that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @return a list of entities matching the supplied criteria; null means no matching entities in the metadata
     * collection.
     * @throws InvalidParameterException a parameter is invalid or null.
     * @throws TypeErrorException the type guid passed on the request is not known by the
     *                              metadata collection.
     * @throws RepositoryErrorException there is a problem communicating with the metadata repository where
     *                                    the metadata collection is stored.
     * @throws ClassificationErrorException the classification request is not known to the metadata collection.
     * @throws PropertyErrorException the properties specified are not valid for the requested type of
     *                                  classification.
     * @throws PagingErrorException the paging/sequencing parameters are set up incorrectly.
     * @throws FunctionNotSupportedException the repository does not support the asOfTime parameter.
     * @throws UserNotAuthorizedException the userId is not permitted to perform this operation.
     */
    @Override
    public  List<EntityDetail> findEntitiesByClassification(String                    userId,
                                                            String                    entityTypeGUID,
                                                            String                    classificationName,
                                                            InstanceProperties        matchClassificationProperties,
                                                            MatchCriteria             matchCriteria,
                                                            int                       fromEntityElement,
                                                            List<InstanceStatus>      limitResultsByStatus,
                                                            Date                      asOfTime,
                                                            String                    sequencingProperty,
                                                            SequencingOrder           sequencingOrder,
                                                            int                       pageSize) throws InvalidParameterException,
            TypeErrorException,
            RepositoryErrorException,
            ClassificationErrorException,
            PropertyErrorException,
            PagingErrorException,
            FunctionNotSupportedException,
            UserNotAuthorizedException {

        final String methodName = "findEntitiesByClassification";
        findEntitiesByClassificationParameterValidation(
                userId,
                entityTypeGUID,
                classificationName,
                matchClassificationProperties,
                matchCriteria,
                fromEntityElement,
                limitResultsByStatus,
                asOfTime,
                sequencingProperty,
                sequencingOrder,
                pageSize
        );

        AtlasSearchResult results;

        // Immediately throw unimplemented exception if trying to retrieve historical view
        if (asOfTime != null) {
            OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
            String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                    this.getClass().getName(),
                    repositoryName);
            throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                    this.getClass().getName(),
                    methodName,
                    errorMessage,
                    errorCode.getSystemAction(),
                    errorCode.getUserAction());
        } else if (sequencingOrder != null) {

            List<String> limitResultsByClassification = new ArrayList<>();
            limitResultsByClassification.add(classificationName);

            // Run the base search first (and if we need to match on classification properties, increase pageSize
            // so there is buffer to cull later)
            results = buildAndRunDSLSearch(
                    methodName,
                    entityTypeGUID,
                    limitResultsByClassification,
                    null,
                    matchCriteria,
                    fromEntityElement,
                    limitResultsByStatus,
                    sequencingProperty,
                    sequencingOrder,
                    matchClassificationProperties == null ? pageSize : pageSize * 2
            );
            // TODO: still a risk that there are many classified entities and we overflow beyond this increased pageSize

        } else {

            // Run the base search first (and if we need to match on classification properties, increase pageSize
            // so there is buffer to cull later)
            results = buildAndRunBasicSearch(
                    methodName,
                    entityTypeGUID,
                    classificationName,
                    null,
                    matchCriteria,
                    fromEntityElement,
                    limitResultsByStatus,
                    matchClassificationProperties == null ? pageSize : pageSize * 2
            );
            // TODO: still a risk that there are many classified entities and we overflow beyond this increased pageSize

        }

        // Then limit any results based on the classification properties (if any were provided)
        List<AtlasEntityHeader> atlasEntities = new ArrayList<>();
        if (results != null && matchClassificationProperties != null) {

            Map<String, InstancePropertyValue> propertiesToMatch = matchClassificationProperties.getInstanceProperties();

            List<AtlasEntityHeader> candidateEntities = results.getEntities();
            // For each entity we've preliminarily identified...
            for (AtlasEntityHeader candidateEntity : candidateEntities) {
                List<AtlasClassification> classificationsForEntity = candidateEntity.getClassifications();
                // ... iterate through each of its classifications to narrow in on only the one of interest
                for (AtlasClassification candidateClassification : classificationsForEntity) {
                    if (candidateClassification.getTypeName().equals(classificationName)) {
                        // ... then iterate through the properties of that classification we're trying to match
                        boolean bMatchesAny = false;
                        boolean bMatchesAll = true;
                        for (Map.Entry<String, InstancePropertyValue> propertyToMatch : propertiesToMatch.entrySet()) {
                            String propertyName = propertyToMatch.getKey();
                            InstancePropertyValue omrsPropertyValueToMatch = propertyToMatch.getValue();
                            // Remember that classifications (and their properties) are one-to-one with Egeria (no mapping needed)
                            Object atlasClassificationValue = candidateClassification.getAttribute(propertyName);
                            boolean bMatchesThisOne = AttributeMapping.valuesMatch(omrsPropertyValueToMatch, atlasClassificationValue);
                            bMatchesAll = bMatchesAll && bMatchesThisOne;
                            bMatchesAny = bMatchesAny || bMatchesThisOne;
                        }
                        if (matchCriteria != null) {
                            switch (matchCriteria) {
                                case NONE:
                                    if (!bMatchesAny) {
                                        atlasEntities.add(candidateEntity);
                                    }
                                    break;
                                case ALL:
                                    if (bMatchesAll) {
                                        atlasEntities.add(candidateEntity);
                                    }
                                    break;
                                case ANY:
                                    if (bMatchesAny) {
                                        atlasEntities.add(candidateEntity);
                                    }
                                    break;
                            }
                        } else if (bMatchesAll) {
                            atlasEntities.add(candidateEntity);
                        } else if (log.isDebugEnabled()) {
                            log.debug("Unable to match properties '{}' for entity, dropping from results: {}", matchClassificationProperties, candidateEntity);
                        }
                    }
                }
            }

            // ... trim the results list to size, before we retrieve full EntityDetails for each
            int endOfPageMarker = Math.min(fromEntityElement + pageSize, atlasEntities.size());
            if (fromEntityElement != 0 || endOfPageMarker < atlasEntities.size()) {
                atlasEntities = atlasEntities.subList(fromEntityElement, endOfPageMarker);
                results.setEntities(atlasEntities);
            }

        } else if (results != null) {
            // If no classification properties to limit, just grab the results directly
            atlasEntities = results.getEntities();
        }

        List<EntityDetail> entityDetails = getEntityDetailsFromAtlasResults(atlasEntities, userId);
        return (entityDetails == null || entityDetails.isEmpty()) ? null : entityDetails;

    }

    /**
     * Build an Atlas domain-specific language (DSL) query based on the provided parameters, and return its results.
     *
     * @param entityTypeGUID unique identifier for the type of entity requested.  Null means any type of entity
     *                       (but could be slow so not recommended.
     * @param limitResultsByClassification list of classifications by which to limit the results.
     * @param matchProperties Optional list of entity properties to match (contains wildcards).
     * @param matchCriteria Enum defining how the match properties should be matched to the classifications in the repository.
     * @param fromEntityElement the starting element number of the entities to return.
     *                                This is used when retrieving elements
     *                                beyond the first page of results. Zero means start from the first element.
     * @param limitResultsByStatus By default, entities in all statuses are returned.  However, it is possible
     *                             to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                             status values.
     * @param sequencingProperty String name of the entity property that is to be used to sequence the results.
     *                           Null means do not sequence on a property name (see SequencingOrder).
     * @param sequencingOrder Enum defining how the results should be ordered.
     * @param pageSize the maximum number of result entities that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @return {@code List<EntityDetail>}
     * @throws FunctionNotSupportedException
     * @throws InvalidParameterException
     * @throws RepositoryErrorException
     * @throws UserNotAuthorizedException
     */
    private AtlasSearchResult buildAndRunDSLSearch(String methodName,
                                                   String entityTypeGUID,
                                                   List<String> limitResultsByClassification,
                                                   InstanceProperties matchProperties,
                                                   MatchCriteria matchCriteria,
                                                   int fromEntityElement,
                                                   List<InstanceStatus> limitResultsByStatus,
                                                   String sequencingProperty,
                                                   SequencingOrder sequencingOrder,
                                                   int pageSize) throws
            FunctionNotSupportedException {

        // If we need to order the results, it will probably be more efficient to use Atlas's DSL query language
        // to do the search
        StringBuffer sb = new StringBuffer();

        // For this kind of query, we MUST have an entity type (for Atlas),
        // so will default to Referenceable if nothing else was specified
        String omrsTypeName = "Referenceable";
        String atlasTypeName = omrsTypeName;
        if (entityTypeGUID != null) {
            TypeDef typeDef = typeDefStore.getTypeDefByGUID(entityTypeGUID);
            if (typeDef != null) {
                omrsTypeName = typeDef.getName();
                atlasTypeName = typeDefStore.getMappedAtlasTypeDefName(omrsTypeName);
            } else {
                if (log.isWarnEnabled()) { log.warn("Unable to search for type, unknown to repository: {}", entityTypeGUID); }
            }
        }
        Map<String, String> omrsPropertyMap = typeDefStore.getPropertyMappingsForOMRSTypeDef(omrsTypeName);
        sb.append("from " + atlasTypeName);
        boolean bWhereClauseAdded = false;

        // Add the multiple classification criteria, if requested
        // (recall that OMRS classification name should be identical to Atlas classification name -- no translation needed)
        if (limitResultsByClassification != null) {
            List<String> classifications = new ArrayList<>();
            for (String classificationName : limitResultsByClassification) {
                classifications.add(atlasTypeName + " isa " + classificationName);
            }
            if (!classifications.isEmpty()) {
                sb.append(" where ");
                sb.append(String.join(" and ", classifications));
                bWhereClauseAdded = true;
            }
        }

        // Add match properties, if requested
        if (matchProperties != null) {
            List<String> propertyCriteria = new ArrayList<>();
            Map<String, InstancePropertyValue> properties = matchProperties.getInstanceProperties();
            // By default, include only Referenceable's properties (as these will be the only properties that exist
            // across ALL entity types)
            Map<String, TypeDefAttribute> omrsAttrTypeDefs = typeDefStore.getAllTypeDefAttributesForName(omrsTypeName);
            if (properties != null) {
                for (Map.Entry<String, InstancePropertyValue> property : properties.entrySet()) {
                    String omrsPropertyName = property.getKey();
                    InstancePropertyValue value = property.getValue();
                    addSearchConditionFromValue(
                            propertyCriteria,
                            omrsPropertyName,
                            value,
                            omrsPropertyMap,
                            omrsAttrTypeDefs,
                            (matchCriteria == null ? false : matchCriteria.equals(MatchCriteria.NONE)),
                            true
                    );
                }
            }
            if (!propertyCriteria.isEmpty()) {
                String propertyMatchDelim = " and ";
                if (matchCriteria != null && matchCriteria.equals(MatchCriteria.ANY)) {
                    propertyMatchDelim = " or ";
                }
                if (!bWhereClauseAdded) {
                    sb.append(" where");
                }
                sb.append(" ");
                sb.append(String.join(propertyMatchDelim, propertyCriteria));
            }
        }

        // Add status limiters, if requested
        if (limitResultsByStatus != null) {
            List<String> states = new ArrayList<>();
            Set<InstanceStatus> limitSet = new HashSet<>(limitResultsByStatus);
            if (limitSet.equals(availableStates)) {
                states.add("__state = 'DELETED'");
                states.add("__state = 'ACTIVE'");
            } else if (limitSet.size() == 1 && limitSet.contains(InstanceStatus.DELETED)) {
                states.add("__state = 'DELETED'");
            } else if (limitSet.size() == 1 && limitSet.contains(InstanceStatus.ACTIVE)) {
                states.add("__state = 'ACTIVE'");
            } else {
                // Otherwise we must be searching for states that Atlas does not support
                OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        this.getClass().getName(),
                        repositoryName);
                throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
            if (!states.isEmpty()) {
                if (!bWhereClauseAdded) {
                    sb.append(" where");
                }
                sb.append(" ");
                sb.append(String.join(" or ", states));
            }
        }

        // Add sorting criteria, if requested
        if (sequencingOrder != null) {
            switch (sequencingOrder) {
                case GUID:
                    sb.append(" orderby __guid asc");
                    break;
                case LAST_UPDATE_OLDEST:
                    sb.append(" orderby __modificationTimestamp asc");
                    break;
                case LAST_UPDATE_RECENT:
                    sb.append(" orderby __modificationTimestamp desc");
                    break;
                case CREATION_DATE_OLDEST:
                    sb.append(" orderby __timestamp asc");
                    break;
                case CREATION_DATE_RECENT:
                    sb.append(" orderby __timestamp desc");
                    break;
                case PROPERTY_ASCENDING:
                    if (sequencingProperty != null) {
                        String atlasPropertyName = omrsPropertyMap.get(sequencingProperty);
                        if (atlasPropertyName != null) {
                            sb.append(" orderby ");
                            sb.append(atlasPropertyName);
                            sb.append(" asc");
                        } else {
                            if (log.isWarnEnabled()) { log.warn("Unable to find mapped Atlas property for sorting for: {}", sequencingProperty); }
                            sb.append(" orderby __guid asc");
                        }
                    } else {
                        if (log.isWarnEnabled()) { log.warn("No property for sorting provided, defaulting to GUID."); }
                        sb.append(" orderby __guid asc");
                    }
                    break;
                case PROPERTY_DESCENDING:
                    if (sequencingProperty != null) {
                        String atlasPropertyName = omrsPropertyMap.get(sequencingProperty);
                        if (atlasPropertyName != null) {
                            sb.append(" orderby ");
                            sb.append(atlasPropertyName);
                            sb.append(" desc");
                        } else {
                            if (log.isWarnEnabled()) { log.warn("Unable to find mapped Atlas property for sorting for: {}", sequencingProperty); }
                            sb.append(" orderby __guid asc");
                        }
                    } else {
                        if (log.isWarnEnabled()) { log.warn("No property for sorting provided, defaulting to GUID."); }
                        sb.append(" orderby __guid desc");
                    }
                    break;
                default:
                    // Do nothing -- no sorting
                    break;
            }
        }

        // Add paging criteria, if requested
        if (pageSize > 0) {
            sb.append(" limit " + pageSize);
        }
        if (fromEntityElement > 0) {
            sb.append(" offset " + fromEntityElement);
        }

        return atlasRepositoryConnector.searchWithDSL(sb.toString());

    }

    /**
     * Build an Atlas basic search based on the provided parameters, and return its results.
     *
     * @param entityTypeGUID unique identifier for the type of entity requested.  Null means any type of entity
     *                       (but could be slow so not recommended.
     * @param limitResultsByClassification name of a single classification by which to limit the results.
     * @param matchProperties Optional list of entity properties to match (contains wildcards).
     * @param matchCriteria Enum defining how the match properties should be matched to the classifications in the repository.
     * @param fromEntityElement the starting element number of the entities to return.
     *                                This is used when retrieving elements
     *                                beyond the first page of results. Zero means start from the first element.
     * @param limitResultsByStatus By default, entities in all statuses are returned.  However, it is possible
     *                             to specify a list of statuses (eg ACTIVE) to restrict the results to.  Null means all
     *                             status values.
     * @param pageSize the maximum number of result entities that can be returned on this request.  Zero means
     *                 unrestricted return results size.
     * @return {@code List<EntityDetail>}
     * @throws FunctionNotSupportedException
     */
    private AtlasSearchResult buildAndRunBasicSearch(String methodName,
                                                     String entityTypeGUID,
                                                     String limitResultsByClassification,
                                                     InstanceProperties matchProperties,
                                                     MatchCriteria matchCriteria,
                                                     int fromEntityElement,
                                                     List<InstanceStatus> limitResultsByStatus,
                                                     int pageSize) throws
            FunctionNotSupportedException {

        // Otherwise Atlas's "basic" search is likely to be significantly faster
        SearchParameters searchParameters = new SearchParameters();
        searchParameters.setIncludeClassificationAttributes(true);
        searchParameters.setIncludeSubClassifications(true);
        searchParameters.setIncludeSubTypes(true);
        searchParameters.setOffset(fromEntityElement);
        searchParameters.setLimit(pageSize);

        String omrsTypeName = "Referenceable";
        if (entityTypeGUID != null) {
            TypeDef typeDef = typeDefStore.getTypeDefByGUID(entityTypeGUID);
            if (typeDef != null) {
                omrsTypeName = typeDef.getName();
                String atlasTypeName = typeDefStore.getMappedAtlasTypeDefName(omrsTypeName);
                searchParameters.setTypeName(atlasTypeName);
            } else {
                if (log.isWarnEnabled()) {
                    log.warn("Unable to search for type, unknown to repository: {}", entityTypeGUID);
                }
            }
        }

        if (matchProperties != null) {
            Map<String, InstancePropertyValue> properties = matchProperties.getInstanceProperties();
            // By default, include only Referenceable's properties (as these will be the only properties that exist
            // across ALL entity types)
            Map<String, String> omrsPropertyMap = typeDefStore.getPropertyMappingsForOMRSTypeDef(omrsTypeName);
            Map<String, TypeDefAttribute> omrsAttrTypeDefs = typeDefStore.getAllTypeDefAttributesForName(omrsTypeName);
            if (properties != null) {
                List<SearchParameters.FilterCriteria> criteria = new ArrayList<>();
                for (Map.Entry<String, InstancePropertyValue> property : properties.entrySet()) {
                    String omrsPropertyName = property.getKey();
                    InstancePropertyValue value = property.getValue();
                    addSearchConditionFromValue(
                            criteria,
                            omrsPropertyName,
                            value,
                            omrsPropertyMap,
                            omrsAttrTypeDefs,
                            (matchCriteria == null ? false : matchCriteria.equals(MatchCriteria.NONE)),
                            false
                    );
                }
                SearchParameters.FilterCriteria entityFilters = new SearchParameters.FilterCriteria();
                entityFilters.setCriterion(criteria);
                if (matchCriteria != null) {
                    switch (matchCriteria) {
                        case ALL:
                        case NONE:
                            entityFilters.setCondition(SearchParameters.FilterCriteria.Condition.AND);
                            break;
                        case ANY:
                            entityFilters.setCondition(SearchParameters.FilterCriteria.Condition.OR);
                            break;
                    }
                } else {
                    entityFilters.setCondition(SearchParameters.FilterCriteria.Condition.AND);
                }
                searchParameters.setEntityFilters(entityFilters);
            }
        }

        if (limitResultsByStatus != null) {
            Set<InstanceStatus> limitSet = new HashSet<>(limitResultsByStatus);
            if (limitSet.equals(availableStates) || (limitSet.size() == 1 && limitSet.contains(InstanceStatus.DELETED))) {
                // If we're to search for deleted, do not exclude deleted
                searchParameters.setExcludeDeletedEntities(false);
            } else if (limitSet.size() == 1 && limitSet.contains(InstanceStatus.ACTIVE)) {
                // Otherwise if we are only after active, do exclude deleted
                searchParameters.setExcludeDeletedEntities(true);
            } else {
                // Otherwise we must be searching for states that Atlas does not support
                OMRSErrorCode errorCode = OMRSErrorCode.METHOD_NOT_IMPLEMENTED;
                String errorMessage = errorCode.getErrorMessageId() + errorCode.getFormattedErrorMessage(methodName,
                        this.getClass().getName(),
                        repositoryName);
                throw new FunctionNotSupportedException(errorCode.getHTTPErrorCode(),
                        this.getClass().getName(),
                        methodName,
                        errorMessage,
                        errorCode.getSystemAction(),
                        errorCode.getUserAction());
            }
        }

        if (limitResultsByClassification != null) {
            searchParameters.setClassification(limitResultsByClassification);
        }

        return atlasRepositoryConnector.searchForEntities(searchParameters);

    }

    /**
     * Retrieves a list of EntityDetail objects given a list of AtlasEntityHeader objects.
     *
     * @param atlasEntities the Atlas entities for which to retrieve details
     * @param userId the user through which to do the retrieval
     * @return {@code List<EntityDetail>}
     * @throws InvalidParameterException
     * @throws RepositoryErrorException
     * @throws UserNotAuthorizedException
     */
    private List<EntityDetail> getEntityDetailsFromAtlasResults(List<AtlasEntityHeader> atlasEntities,
                                                                String userId) throws
            InvalidParameterException,
            RepositoryErrorException,
            UserNotAuthorizedException {

        List<EntityDetail> entityDetails = new ArrayList<>();

        if (atlasEntities != null) {
            for (AtlasEntityHeader atlasEntityHeader : atlasEntities) {
                try {
                    EntityDetail detail = getEntityDetail(userId, atlasEntityHeader.getGuid());
                    entityDetails.add(detail);
                } catch (EntityNotKnownException e) {
                    if (log.isErrorEnabled()) {
                        log.error("Entity with GUID {} not known -- excluding from results.", atlasEntityHeader.getGuid());
                    }
                } catch (EntityProxyOnlyException e) {
                    if (log.isErrorEnabled()) {
                        log.error("Entity with GUID {} only a proxy -- excluding from results.", atlasEntityHeader.getGuid());
                    }
                }
            }
        }

        return entityDetails;

    }

    /**
     * Adds the provided value to the search criteria for Apache Atlas.
     *
     * @param criteria the search criteria to which to append
     * @param omrsPropertyName the OMRS property name to search
     * @param value the value for which to search
     * @param omrsToAtlasPropertyMap the mappings from OMRS property name to Atlas property name
     * @param omrsTypeDefAttrMap the mappings from OMRS property name to TypeDefAttribute definition of the property
     * @param negateCondition if true, negate (invert) the condition / operator
     */
    private <T> void addSearchConditionFromValue(List<T> criteria,
                                                 String omrsPropertyName,
                                                 InstancePropertyValue value,
                                                 Map<String, String> omrsToAtlasPropertyMap,
                                                 Map<String, TypeDefAttribute> omrsTypeDefAttrMap,
                                                 boolean negateCondition,
                                                 boolean dslQuery) {

        if (omrsPropertyName != null) {
            String atlasPropertyName = omrsToAtlasPropertyMap.get(omrsPropertyName);
            if (atlasPropertyName != null) {

                SearchParameters.FilterCriteria atlasCriterion = new SearchParameters.FilterCriteria();
                StringBuffer sbCriterion = new StringBuffer();
                InstancePropertyCategory category = value.getInstancePropertyCategory();
                switch (category) {
                    case PRIMITIVE:
                        PrimitivePropertyValue actualValue = (PrimitivePropertyValue) value;
                        PrimitiveDefCategory primitiveType = actualValue.getPrimitiveDefCategory();
                        switch (primitiveType) {
                            case OM_PRIMITIVE_TYPE_BOOLEAN:
                            case OM_PRIMITIVE_TYPE_SHORT:
                            case OM_PRIMITIVE_TYPE_INT:
                            case OM_PRIMITIVE_TYPE_LONG:
                            case OM_PRIMITIVE_TYPE_FLOAT:
                            case OM_PRIMITIVE_TYPE_DOUBLE:
                            case OM_PRIMITIVE_TYPE_BIGINTEGER:
                            case OM_PRIMITIVE_TYPE_BIGDECIMAL:
                                String nonString = actualValue.getPrimitiveValue().toString();
                                atlasCriterion.setAttributeName(atlasPropertyName);
                                sbCriterion.append(atlasPropertyName);
                                if (negateCondition) {
                                    atlasCriterion.setOperator(SearchParameters.Operator.NEQ);
                                    sbCriterion.append(" != ");
                                } else {
                                    atlasCriterion.setOperator(SearchParameters.Operator.EQ);
                                    sbCriterion.append(" = ");
                                }
                                atlasCriterion.setAttributeValue(nonString);
                                sbCriterion.append(nonString);
                                if (dslQuery) {
                                    criteria.add((T) sbCriterion.toString());
                                } else {
                                    criteria.add((T) atlasCriterion);
                                }
                                break;
                            case OM_PRIMITIVE_TYPE_BYTE:
                            case OM_PRIMITIVE_TYPE_CHAR:
                                String single = actualValue.getPrimitiveValue().toString();
                                atlasCriterion.setAttributeName(atlasPropertyName);
                                sbCriterion.append(atlasPropertyName);
                                if (negateCondition) {
                                    atlasCriterion.setOperator(SearchParameters.Operator.NEQ);
                                    sbCriterion.append(" != \"");
                                } else {
                                    atlasCriterion.setOperator(SearchParameters.Operator.EQ);
                                    sbCriterion.append(" = \"");
                                }
                                atlasCriterion.setAttributeValue(single);
                                sbCriterion.append(single);
                                sbCriterion.append("\"");
                                if (dslQuery) {
                                    criteria.add((T) sbCriterion.toString());
                                } else {
                                    criteria.add((T) atlasCriterion);
                                }
                                break;
                            case OM_PRIMITIVE_TYPE_DATE:
                                Date date = (Date) actualValue.getPrimitiveValue();
                                String formattedDate = AttributeMapping.ATLAS_DATE_FORMAT.format(date);
                                atlasCriterion.setAttributeName(atlasPropertyName);
                                sbCriterion.append(atlasPropertyName);
                                if (negateCondition) {
                                    atlasCriterion.setOperator(SearchParameters.Operator.NEQ);
                                    sbCriterion.append(" != \"");
                                } else {
                                    atlasCriterion.setOperator(SearchParameters.Operator.EQ);
                                    sbCriterion.append(" = \"");
                                }
                                atlasCriterion.setAttributeValue(formattedDate);
                                sbCriterion.append(formattedDate);
                                sbCriterion.append("\"");
                                if (dslQuery) {
                                    criteria.add((T) sbCriterion.toString());
                                } else {
                                    criteria.add((T) atlasCriterion);
                                }
                                break;
                            case OM_PRIMITIVE_TYPE_STRING:
                            default:
                                atlasCriterion.setAttributeName(atlasPropertyName);
                                sbCriterion.append(atlasPropertyName);
                                String candidateValue = actualValue.getPrimitiveValue().toString();
                                if (candidateValue.startsWith("*") && candidateValue.endsWith("*")) {
                                    sbCriterion.append(" LIKE \"");
                                    sbCriterion.append(candidateValue);
                                    sbCriterion.append("\"");
                                    atlasCriterion.setOperator(SearchParameters.Operator.CONTAINS);
                                    candidateValue = candidateValue.substring(1, candidateValue.length() - 1);
                                } else if (candidateValue.startsWith("*")) {
                                    sbCriterion.append(" LIKE \"");
                                    sbCriterion.append(candidateValue);
                                    sbCriterion.append("\"");
                                    atlasCriterion.setOperator(SearchParameters.Operator.ENDS_WITH);
                                    candidateValue = candidateValue.substring(1);
                                } else if (candidateValue.endsWith("*")) {
                                    sbCriterion.append(" LIKE \"");
                                    sbCriterion.append(candidateValue);
                                    sbCriterion.append("\"");
                                    atlasCriterion.setOperator(SearchParameters.Operator.STARTS_WITH);
                                    candidateValue = candidateValue.substring(0, candidateValue.length() - 1);
                                } else {
                                    if (negateCondition) {
                                        atlasCriterion.setOperator(SearchParameters.Operator.NEQ);
                                        sbCriterion.append(" != \"");
                                        sbCriterion.append(candidateValue);
                                        sbCriterion.append("\"");
                                    } else {
                                        atlasCriterion.setOperator(SearchParameters.Operator.EQ);
                                        sbCriterion.append(" = \"");
                                        sbCriterion.append(candidateValue);
                                        sbCriterion.append("\"");
                                    }
                                }
                                atlasCriterion.setAttributeValue(candidateValue);
                                if (dslQuery) {
                                    criteria.add((T) sbCriterion.toString());
                                } else {
                                    criteria.add((T) atlasCriterion);
                                }
                                break;
                        }
                        break;
                    case ENUM:
                        String omrsEnumValue = ((EnumPropertyValue) value).getSymbolicName();
                        TypeDefAttribute typeDefAttribute = omrsTypeDefAttrMap.get(omrsPropertyName);
                        if (typeDefAttribute != null) {
                            Map<String, String> elementMap = attributeTypeDefStore.getElementMappingsForOMRSTypeDef(typeDefAttribute.getAttributeName());
                            String atlasEnumValue = elementMap.get(omrsEnumValue);
                            if (atlasEnumValue != null) {
                                atlasCriterion.setAttributeName(atlasPropertyName);
                                sbCriterion.append(atlasPropertyName);
                                if (negateCondition) {
                                    atlasCriterion.setOperator(SearchParameters.Operator.NEQ);
                                    sbCriterion.append(" != \"");
                                } else {
                                    atlasCriterion.setOperator(SearchParameters.Operator.EQ);
                                    sbCriterion.append(" = \"");
                                }
                                atlasCriterion.setAttributeValue(atlasEnumValue);
                                sbCriterion.append(atlasEnumValue);
                                sbCriterion.append("\"");
                                if (dslQuery) {
                                    criteria.add((T) sbCriterion.toString());
                                } else {
                                    criteria.add((T) atlasCriterion);
                                }
                            } else {
                                if (log.isWarnEnabled()) { log.warn("Unable to find mapped enum value for {}: {}", omrsPropertyName, omrsEnumValue); }
                            }
                        } else {
                            if (log.isWarnEnabled()) { log.warn("Unable to find enum with name: {}", omrsPropertyName); }
                        }
                        break;
                    /*case STRUCT:
                        Map<String, InstancePropertyValue> structValues = ((StructPropertyValue) value).getAttributes().getInstanceProperties();
                        for (Map.Entry<String, InstancePropertyValue> nextEntry : structValues.entrySet()) {
                            addSearchConditionFromValue(
                                    igcSearchConditionSet,
                                    nextEntry.getKey(),
                                    igcProperties,
                                    mapping,
                                    nextEntry.getValue()
                            );
                        }
                        break;*/
                    case MAP:
                        Map<String, InstancePropertyValue> mapValues = ((MapPropertyValue) value).getMapValues().getInstanceProperties();
                        for (Map.Entry<String, InstancePropertyValue> nextEntry : mapValues.entrySet()) {
                            addSearchConditionFromValue(
                                    criteria,
                                    nextEntry.getKey(),
                                    nextEntry.getValue(),
                                    omrsToAtlasPropertyMap,
                                    omrsTypeDefAttrMap,
                                    negateCondition,
                                    dslQuery
                            );
                        }
                        break;
                    case ARRAY:
                        Map<String, InstancePropertyValue> arrayValues = ((ArrayPropertyValue) value).getArrayValues().getInstanceProperties();
                        for (Map.Entry<String, InstancePropertyValue> nextEntry : arrayValues.entrySet()) {
                            addSearchConditionFromValue(
                                    criteria,
                                    atlasPropertyName,
                                    nextEntry.getValue(),
                                    omrsToAtlasPropertyMap,
                                    omrsTypeDefAttrMap,
                                    negateCondition,
                                    dslQuery
                            );
                        }
                        break;
                    default:
                        // Do nothing
                        if (log.isWarnEnabled()) { log.warn("Unable to handle search criteria for value type: {}", category); }
                        break;
                }

            } else {
                if (log.isWarnEnabled()) { log.warn("Unable to add search condition, no mapped Atlas property for '{}': {}", omrsPropertyName, value); }
            }
        } else {
            if (log.isWarnEnabled()) { log.warn("Unable to add search condition, no OMRS property: {}", value); }
        }

    }

}
