<!-- SPDX-License-Identifier: CC-BY-4.0 -->
<!-- Copyright Contributors to the ODPi Egeria project. -->

# Implemented mappings

The following types are currently mapped from Apache Atlas to OMRS. Note that there are currently limited
mappings from OMRS types to Apache Atlas types as this connector is primarily read-only (primarily capable of
propagating or retrieving information _from_ Apache Atlas, and not _to_ Apache Atlas).

Hoping for a mapping that isn't there?

- [Submit an issue](https://github.com/odpi/egeria-connector-apache-atlas/issues/new), or
- Check out any of the linked files below for examples of what's needed to create a mapping,
    and create your own (and feel free to submit a PR with the result!)

## Enumerations

Mappings for enumerations are defined directly in the
[EnumDefMappings.json](../../adapter/src/main/resources/EnumDefMappings.json) file.

This file is a simple array of JSON objects, each of which defines:

- the name of the Apache Atlas enumeration (keyed by `atlas`)
- the name of the Open Metadata Type enumeration (keyed by `omrs`)
- an array of value mappings (keyed by `propertyMappings`)
    - each element of which is a name-value pair between the Apache Atlas value (`atlas`) and the Open Metadata value (`omrs`)

In addition to these mapped enumerations, any enumerations defined in the Open Metadata types that are not mapped will
also automatically be created within Apache Atlas (using their same name and valid values as in the open metadata types
themselves.)

## Type definitions

The mappings for both Entity and Relationship TypeDefs are defined directly in the
[TypeDefMappings.json](../../adapter/src/main/resources/TypeDefMappings.json) file.

Like the enumeration mappings, this file is a relatively simple array of JSON objects, each of which defines:

- the name of the Apache Atlas type definition (keyed by `atlas`)
- the name of the Open Metadata type definition (keyed by `omrs`)
- an (optional) unique prefix value, for any Open Metadata types that do not exist as distinct entities in Apache Atlas
    but for which we should generate an entity in Open Metadata (keyed by `prefix`)
- an array of property mappings (keyed by `propertyMappings`):
    - each element of which is a name-value pair between the Apache Atlas property name (`atlas`) and the Open Metadata
        property name (`omrs`)
- for relationship type definitions, an array of endpoint mappings (keyed by `endpointMappings`):
    - which must always have 2 elements, one representing each endpoint of the relationship
    - each containing the name of the Apache Atlas relationship endpoint (`atlas`) and the name of the Open Metadata
        endpoint of the relationship (`omrs`)
    - in the case of relationships for generated entities, may instead of the `atlas` endpoint (since there is not one)
        specify the `prefix` for the generated entity for that end of the relationship

For any Open Metadata entity types that do not exist in Apache Atlas, they will not be automatically created -- only
those that are mapped through the file defined above will exist.

## Classifications

Because Apache Atlas does not ship with any out-of-the-box Classifications, but has a "Classification" concept, the
connector automatically creates any open metadata Classifications that are applicable to the types in Apache Atlas as
part of its cohort registration process. Therefore the mappings between the open metadata Classifications and these
auto-created Classifications are one-to-one -- all names, properties, and endpoints are identical within Apache Atlas
to what is defined in the open metadata Classification itself.


----
License: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/),
Copyright Contributors to the ODPi Egeria project.
