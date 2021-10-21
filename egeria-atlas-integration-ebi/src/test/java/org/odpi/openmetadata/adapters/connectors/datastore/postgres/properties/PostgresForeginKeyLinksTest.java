package org.odpi.openmetadata.adapters.connectors.datastore.postgres.properties;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PostgresForeginKeyLinksTest {

    @DisplayName("Test getImportedColumnQualifiedName()")
    @Test
    void getImportedColumnQualifiedName() {

        PostgresForeginKeyLinks link = new PostgresForeginKeyLinks(     "table_schema",
                                                                    "constraint_name",
                                                                        "table_name",
                                                                        "column_name",
                                                                    "foreign_schema",
                                                                    "foregin_table",
                                                                    "foregin_column");


        assertEquals( link.getImportedColumnQualifiedName(), "table_schema" + "." + "table_name" + "." + "column_name");
    }

    @DisplayName("Test getExportedColumnQualifiedName()")
    @Test
    void getExportedColumnQualifiedName() {

        PostgresForeginKeyLinks link = new PostgresForeginKeyLinks(     "table_schema",
                "constraint_name",
                "table_name",
                "column_name",
                "foregin_schema",
                "foregin_table",
                "foregin_column");

        assertEquals( link.getExportedColumnQualifiedName(), "foregin_schema" + "." + "foregin_table" + "." + "foregin_column");

    }
}