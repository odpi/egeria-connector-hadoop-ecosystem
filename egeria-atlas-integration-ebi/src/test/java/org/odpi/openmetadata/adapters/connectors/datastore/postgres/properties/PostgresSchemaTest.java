package org.odpi.openmetadata.adapters.connectors.datastore.postgres.properties;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgresSchemaTest {


    void PostgresSchemaTest()
    {
    }

    @Test
    void getCatalog_name() {
        PostgresSchema schema =  new PostgresSchema(   "catalog_name",
                "schema_name",
                "schema_owner",
                "default_catalog_character_set",
                "default_schema_character_set",
                "default_character_set_name",
                "sql_path");

        assertEquals( schema.getCatalog_name(), "catalog_name" );
    }

    @Test
    void getSchema_name() {
        PostgresSchema schema =  new PostgresSchema(   "catalog_name",
                "schema_name",
                "schema_owner",
                "default_catalog_character_set",
                "default_schema_character_set",
                "default_character_set_name",
                "sql_path");

        assertEquals( schema.getSchema_name(), "schema_name" );
    }

    @Test
    void getSchema_owner() {
        PostgresSchema schema =  new PostgresSchema(   "catalog_name",
                "schema_name",
                "schema_owner",
                "default_catalog_character_set",
                "default_schema_character_set",
                "default_character_set_name",
                "sql_path");

        assertEquals( schema.getSchema_owner(), "schema_owner" );
    }

    @Test
    void getDefault_character_set_catalog() {
        PostgresSchema schema =  new PostgresSchema(   "catalog_name",
                "schema_name",
                "schema_owner",
                "default_catalog_character_set",
                "default_schema_character_set",
                "default_character_set_name",
                "sql_path");

        assertEquals(schema.getDefault_character_set_catalog(), "default_catalog_character_set");
    }

    @Test
    void getDefault_character_set_schema() {
        PostgresSchema schema =  new PostgresSchema(   "catalog_name",
                "schema_name",
                "schema_owner",
                "default_catalog_character_set",
                "default_schema_character_set",
                "default_character_set_name",
                "sql_path");
        assertEquals( schema.getDefault_character_set_schema(), "default_schema_character_set");
    }


    @Test
    void getDefault_character_set_name() {
        PostgresSchema schema =  new PostgresSchema(   "catalog_name",
                "schema_name",
                "schema_owner",
                "default_catalog_character_set",
                "default_schema_character_set",
                "default_character_set_name",
                "sql_path");
          assertEquals(schema.getDefault_character_set_name(), "default_character_set_name");
  }

    @Test
    void getSql_path() {
        PostgresSchema schema =  new PostgresSchema(   "catalog_name",
                "schema_name",
                "schema_owner",
                "default_catalog_character_set",
                "default_schema_character_set",
                "default_character_set_name",
                "sql_path");
           assertEquals(schema.getSql_path(), "sql_path");
   }

    @Test
    void getProperties() {
        PostgresSchema schema =  new PostgresSchema(   "catalog_name",
                "schema_name",
                "schema_owner",
                "default_catalog_character_set",
                "default_schema_character_set",
                "default_character_set_name",
                "sql_path");

        Map<String,String> testProps = new HashMap<>();
        testProps.put("default_character_set_name", "default_character_set_name" );
        testProps.put("catalog_name","catalog_name");
        testProps.put("schema_owner", "schema_owner" );
        testProps.put("sql_path", "sql_path" );
        testProps.put("default_character_set_catalog", "default_catalog_character_set");
        testProps.put("schema_name", "schema_name" );
        testProps.put("default_character_set_schema", "default_schema_character_set");

        Map<String, String> props = schema.getProperties();
        assertEquals( props.size(), 7);
        Boolean b = props.entrySet().stream()
                .allMatch(e -> e.getValue().equals(testProps.get(e.getKey())));
        assertTrue( b);

    }

    @Test
    void getQualifiedName() {
        PostgresSchema schema =  new PostgresSchema(   "catalog_name",
                "schema_name",
                "schema_owner",
                "default_catalog_character_set",
                "default_schema_character_set",
                "default_character_set_name",
                "sql_path");

        assertEquals(schema.getQualifiedName(), "catalog_name" + "." + "schema_name" + "." + "sql_path");
    }
}