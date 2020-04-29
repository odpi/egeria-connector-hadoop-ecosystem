/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector;

import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.mapping.AttributeMapping;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.instances.PrimitivePropertyValue;
import org.odpi.openmetadata.repositoryservices.connectors.stores.metadatacollectionstore.properties.typedefs.PrimitiveDefCategory;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import static org.testng.Assert.*;

public class AttributeMappingTest {

    @Test
    public void testEquivalentValues() {

        testEquivalence(false, true, PrimitiveDefCategory.OM_PRIMITIVE_TYPE_BOOLEAN);
        testEquivalence((byte)1, (byte)2, PrimitiveDefCategory.OM_PRIMITIVE_TYPE_BYTE);
        testEquivalence('a', 'b', PrimitiveDefCategory.OM_PRIMITIVE_TYPE_CHAR);
        testEquivalence((short)1, (short)2, PrimitiveDefCategory.OM_PRIMITIVE_TYPE_SHORT);
        testEquivalence(3, 4, PrimitiveDefCategory.OM_PRIMITIVE_TYPE_INT);
        testEquivalence(5L, 6L, PrimitiveDefCategory.OM_PRIMITIVE_TYPE_LONG);
        testEquivalence((float)7.0, (float)7.1, PrimitiveDefCategory.OM_PRIMITIVE_TYPE_FLOAT);
        testEquivalence(8.0, 8.1, PrimitiveDefCategory.OM_PRIMITIVE_TYPE_DOUBLE);
        testEquivalence(BigInteger.valueOf(9), BigInteger.valueOf(10), PrimitiveDefCategory.OM_PRIMITIVE_TYPE_BIGINTEGER);
        testEquivalence(BigDecimal.valueOf(11.0), BigDecimal.valueOf(11.1), PrimitiveDefCategory.OM_PRIMITIVE_TYPE_BIGDECIMAL);
        long now = new Date().getTime();
        testEquivalence(now, now + 1, PrimitiveDefCategory.OM_PRIMITIVE_TYPE_DATE);
        testEquivalence("String1", "String2", PrimitiveDefCategory.OM_PRIMITIVE_TYPE_STRING);

    }

    private void testEquivalence(Object test, Object otherValue, PrimitiveDefCategory category) {
        PrimitivePropertyValue testIPV = new PrimitivePropertyValue();
        testIPV.setPrimitiveDefCategory(category);
        testIPV.setPrimitiveValue(test);
        assertTrue(AttributeMapping.valuesMatch(testIPV, test));
        assertFalse(AttributeMapping.valuesMatch(testIPV, otherValue));
        testComparator(test, otherValue, category);
    }

    private void testComparator(Object first, Object second, PrimitiveDefCategory category) {
        PrimitivePropertyValue one = new PrimitivePropertyValue();
        one.setPrimitiveDefCategory(category);
        one.setPrimitiveValue(first);
        PrimitivePropertyValue two = new PrimitivePropertyValue();
        two.setPrimitiveDefCategory(category);
        two.setPrimitiveValue(second);
        assertTrue(AttributeMapping.compareInstanceProperty(one, two) < 0);
        assertTrue(AttributeMapping.compareInstanceProperty(two, one) > 0);
        assertEquals(AttributeMapping.compareInstanceProperty(one, one), 0);
    }

}
