/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping;

import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.InstanceProperties;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.*;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.repositoryconnector.OMRSRepositoryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * The base class for all mappings between OMRS AttributeTypeDefs and Apache Atlas properties.
 */
public abstract class AttributeMapping {

    private static final Logger log = LoggerFactory.getLogger(AttributeMapping.class);

    /**
     * Add the supplied property to an instance properties object.  If the instance property object
     * supplied is null, a new instance properties object is created.
     *
     * @param omrsRepositoryHelper the OMRS repository helper
     * @param sourceName  name of caller
     * @param properties  properties object to add property to may be null.
     * @param property  the property
     * @param propertyValue  value of property
     * @param methodName  calling method name
     * @return instance properties object.
     */
    public static InstanceProperties addPrimitivePropertyToInstance(OMRSRepositoryHelper omrsRepositoryHelper,
                                                                    String sourceName,
                                                                    InstanceProperties properties,
                                                                    TypeDefAttribute property,
                                                                    Object propertyValue,
                                                                    String methodName) {

        InstanceProperties  resultingProperties = properties;

        if (propertyValue != null) {
            String propertyName = property.getAttributeName();
            if (log.isDebugEnabled()) { log.debug("Adding property " + propertyName + " for " + methodName); }

            if (property.getAttributeType().getCategory() == AttributeTypeDefCategory.PRIMITIVE) {
                try {
                    PrimitiveDef primitiveDef = (PrimitiveDef) property.getAttributeType();
                    switch (primitiveDef.getPrimitiveDefCategory()) {
                        case OM_PRIMITIVE_TYPE_BOOLEAN:
                            boolean booleanValue;
                            if (propertyValue instanceof Boolean) {
                                booleanValue = (Boolean) propertyValue;
                            } else {
                                booleanValue = Boolean.valueOf(propertyValue.toString());
                            }
                            resultingProperties = omrsRepositoryHelper.addBooleanPropertyToInstance(
                                    sourceName,
                                    properties,
                                    propertyName,
                                    booleanValue,
                                    methodName
                            );
                            break;
                        case OM_PRIMITIVE_TYPE_INT:
                            int intValue;
                            if (propertyValue instanceof Integer) {
                                intValue = (Integer) propertyValue;
                            } else if (propertyValue instanceof Number) {
                                intValue = ((Number) propertyValue).intValue();
                            } else {
                                intValue = Integer.valueOf(propertyValue.toString());
                            }
                            resultingProperties = omrsRepositoryHelper.addIntPropertyToInstance(
                                    sourceName,
                                    properties,
                                    propertyName,
                                    intValue,
                                    methodName
                            );
                            break;
                        case OM_PRIMITIVE_TYPE_LONG:
                            long longValue;
                            if (propertyValue instanceof Long) {
                                longValue = (Long) propertyValue;
                            } else if (propertyValue instanceof Number) {
                                longValue = ((Number) propertyValue).longValue();
                            } else {
                                longValue = Long.valueOf(propertyValue.toString());
                            }
                            resultingProperties = omrsRepositoryHelper.addLongPropertyToInstance(
                                    sourceName,
                                    properties,
                                    propertyName,
                                    longValue,
                                    methodName
                            );
                            break;
                        case OM_PRIMITIVE_TYPE_FLOAT:
                            float floatValue;
                            if (propertyValue instanceof Float) {
                                floatValue = (Float) propertyValue;
                            } else if (propertyValue instanceof Number) {
                                floatValue = ((Number) propertyValue).floatValue();
                            } else {
                                floatValue = Float.valueOf(propertyValue.toString());
                            }
                            resultingProperties = omrsRepositoryHelper.addFloatPropertyToInstance(
                                    sourceName,
                                    properties,
                                    propertyName,
                                    floatValue,
                                    methodName
                            );
                            break;
                        case OM_PRIMITIVE_TYPE_STRING:
                            String stringValue;
                            if (propertyValue instanceof String) {
                                stringValue = (String) propertyValue;
                            } else {
                                stringValue = propertyValue.toString();
                            }
                            resultingProperties = omrsRepositoryHelper.addStringPropertyToInstance(
                                    sourceName,
                                    properties,
                                    propertyName,
                                    stringValue,
                                    methodName
                            );
                            break;
                        case OM_PRIMITIVE_TYPE_DATE:
                            if (propertyValue instanceof Date) {
                                resultingProperties = omrsRepositoryHelper.addDatePropertyToInstance(
                                        sourceName,
                                        properties,
                                        propertyName,
                                        (Date) propertyValue,
                                        methodName
                                );
                            } else {
                                if (log.isWarnEnabled()) { log.warn("Unable to parse date automatically -- must be first converted before passing in: {}", propertyValue); }
                            }
                            break;
                        default:
                            if (log.isErrorEnabled()) { log.error("Unhandled primitive type {} for {}", primitiveDef.getPrimitiveDefCategory(), propertyName); }
                    }
                } catch (ClassCastException e) {
                    if (log.isErrorEnabled()) { log.error("Unable to cast {} to {} for {}", propertyValue, property.getAttributeType(), propertyName); }
                } catch (NumberFormatException e) {
                    if (log.isWarnEnabled()) { log.warn("Unable to convert {} to {} for {}", propertyValue, property.getAttributeType(), propertyName); }
                }
            } else {
                if (log.isErrorEnabled()) { log.error("Cannot translate non-primitive property {} this way.", propertyName); }
            }
        } else {
            if (log.isDebugEnabled()) { log.debug("Null property"); }
        }

        return resultingProperties;

    }

}
