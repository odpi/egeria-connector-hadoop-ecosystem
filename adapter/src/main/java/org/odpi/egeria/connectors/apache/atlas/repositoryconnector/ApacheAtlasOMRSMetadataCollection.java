/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector;

import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping.ClassificationDefMapping;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping.EntityMapping;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.instances.EntityInstance;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.types.AttributeDef;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.types.ClassificationTypeDef;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.types.TypeDefHeader;
import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.stores.TypeDefStore;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.OMRSMetadataCollectionBase;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.EntityDetail;
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

            TypeDefHeader atlasTypeDef = atlasRepositoryConnector.getTypeDefByName(omrsTypeDefName);
            if (atlasTypeDef != null) {
                // If the TypeDef already exists in Atlas, add it to our store
                // TODO: should really still verify it, in case TypeDef changes
                typeDefStore.addTypeDef(newTypeDef);
            } else {
                // Otherwise, if it is a Classification, we'll add it to Atlas itself
                ClassificationDefMapping.addClassificationToAtlas(
                        (ClassificationDef) newTypeDef,
                        typeDefStore,
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

        // Lookup the basic asset based on the RID (strip off prefix (indicating a generated type), if there)
        EntityInstance entity = this.atlasRepositoryConnector.getEntityByGUID(guid);
        EntityMapping mapping = new EntityMapping(atlasRepositoryConnector, typeDefStore, entity, userId);
        return mapping.getEntityDetail();

    }

}
