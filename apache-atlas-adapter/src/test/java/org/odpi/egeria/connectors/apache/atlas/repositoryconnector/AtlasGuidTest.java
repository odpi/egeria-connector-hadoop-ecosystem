/* SPDX-License-Identifier: Apache-2.0 */
/* Copyright Contributors to the ODPi Egeria project. */
package org.odpi.egeria.connectors.apache.atlas.repositoryconnector;

import org.odpi.egeria.connectors.apache.atlas.repositoryconnector.model.AtlasGuid;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Test the GUID handling.
 */
public class AtlasGuidTest {

    @Test
    public void testComparisons() {

        String guid = "abc123";
        AtlasGuid one = AtlasGuid.fromGuid(guid);
        AtlasGuid two = AtlasGuid.fromGuid(guid);
        assertEquals(one, one);
        assertEquals(one, two);
        assertNotEquals(one, new Object());
        assertEquals(one.hashCode(), two.hashCode());

    }

    @Test
    public void testParsing() {

        assertNull(AtlasGuid.fromGuid(null));

    }

}
