/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping;

import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.SequencingOrder;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.EntityDetail;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.InstanceProperties;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.InstancePropertyValue;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.Relationship;

import java.util.Comparator;

public class SequencingUtils {

    public static final Comparator<Relationship> getRelationshipComparator(SequencingOrder sequencingOrder,
                                                                           String sequencingProperty) {

        Comparator<Relationship> comparator = null;
        if (sequencingOrder != null) {
            switch (sequencingOrder) {
                case GUID:
                    comparator = Comparator.comparing(Relationship::getGUID);
                    break;
                case LAST_UPDATE_OLDEST:
                    comparator = Comparator.comparing(Relationship::getUpdateTime);
                    break;
                case LAST_UPDATE_RECENT:
                    comparator = Comparator.comparing(Relationship::getUpdateTime).reversed();
                    break;
                case CREATION_DATE_OLDEST:
                    comparator = Comparator.comparing(Relationship::getCreateTime);
                    break;
                case CREATION_DATE_RECENT:
                    comparator = Comparator.comparing(Relationship::getCreateTime).reversed();
                    break;
                case PROPERTY_ASCENDING:
                    if (sequencingProperty != null) {
                        comparator = (a, b) -> {
                            InstanceProperties p1 = a.getProperties();
                            InstanceProperties p2 = b.getProperties();
                            InstancePropertyValue v1 = null;
                            InstancePropertyValue v2 = null;
                            if (p1 != null) {
                                v1 = p1.getPropertyValue(sequencingProperty);
                            }
                            if (p2 != null) {
                                v2 = p2.getPropertyValue(sequencingProperty);
                            }
                            return AttributeMapping.compareInstanceProperty(v1, v2);
                        };
                    }
                    break;
                case PROPERTY_DESCENDING:
                    if (sequencingProperty != null) {
                        comparator = (b, a) -> {
                            InstanceProperties p1 = a.getProperties();
                            InstanceProperties p2 = b.getProperties();
                            InstancePropertyValue v1 = null;
                            InstancePropertyValue v2 = null;
                            if (p1 != null) {
                                v1 = p1.getPropertyValue(sequencingProperty);
                            }
                            if (p2 != null) {
                                v2 = p2.getPropertyValue(sequencingProperty);
                            }
                            return AttributeMapping.compareInstanceProperty(v1, v2);
                        };
                    }
                    break;
                default:
                    // Do nothing -- no sorting
                    break;
            }
        }
        return comparator;

    }

    public static final Comparator<EntityDetail> getEntityDetailComparator(SequencingOrder sequencingOrder,
                                                                           String sequencingProperty) {

        Comparator<EntityDetail> comparator = null;
        if (sequencingOrder != null) {
            switch (sequencingOrder) {
                case GUID:
                    comparator = Comparator.comparing(EntityDetail::getGUID);
                    break;
                case LAST_UPDATE_OLDEST:
                    comparator = Comparator.comparing(EntityDetail::getUpdateTime);
                    break;
                case LAST_UPDATE_RECENT:
                    comparator = Comparator.comparing(EntityDetail::getUpdateTime).reversed();
                    break;
                case CREATION_DATE_OLDEST:
                    comparator = Comparator.comparing(EntityDetail::getCreateTime);
                    break;
                case CREATION_DATE_RECENT:
                    comparator = Comparator.comparing(EntityDetail::getCreateTime).reversed();
                    break;
                case PROPERTY_ASCENDING:
                    if (sequencingProperty != null) {
                        comparator = (a, b) -> {
                            InstanceProperties p1 = a.getProperties();
                            InstanceProperties p2 = b.getProperties();
                            InstancePropertyValue v1 = null;
                            InstancePropertyValue v2 = null;
                            if (p1 != null) {
                                v1 = p1.getPropertyValue(sequencingProperty);
                            }
                            if (p2 != null) {
                                v2 = p2.getPropertyValue(sequencingProperty);
                            }
                            return AttributeMapping.compareInstanceProperty(v1, v2);
                        };
                    }
                    break;
                case PROPERTY_DESCENDING:
                    if (sequencingProperty != null) {
                        comparator = (b, a) -> {
                            InstanceProperties p1 = a.getProperties();
                            InstanceProperties p2 = b.getProperties();
                            InstancePropertyValue v1 = null;
                            InstancePropertyValue v2 = null;
                            if (p1 != null) {
                                v1 = p1.getPropertyValue(sequencingProperty);
                            }
                            if (p2 != null) {
                                v2 = p2.getPropertyValue(sequencingProperty);
                            }
                            return AttributeMapping.compareInstanceProperty(v1, v2);
                        };
                    }
                    break;
                default:
                    // Do nothing -- no sorting
                    break;
            }
        }
        return comparator;

    }

}
