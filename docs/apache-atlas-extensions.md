<!-- SPDX-License-Identifier: CC-BY-4.0 -->
<!-- Copyright Contributors to the ODPi Egeria project. -->

# Apache Atlas Extensions

As part of implementing the Apache Atlas Repository Connector, extension have been developed through the software's
type definition extensibility mechanisms.

## Adds properties to Referenceable

Apache Atlas's `Referenceable` entity definition is extended with the following properties:

- `additionalProperties` - a `map<string, string>` to be able to capture any additional properties that might be
    present beyond those defined up-front in a type definition

(The version of the `Referenceable` EntityDef is incremented to `1.3`.)

## Adds supertype to AtlasGlossary

Apache Atlas's `AtlasGlossary` entity definition is extended to also have `Referenceable` as a supertype.

This is to ensure, like in the open metadata types, glossaries include the properties present on `Referenceable`
(like `qualifiedName` and `additionalProperties`) and that they should also be included when searching the repository
for `Referenceable` entities (and all of its subtypes). (Note though that this does not work within Atlas, presumably
due to it still extending `__internal` as well.)

(The version of the `AtlasGlossary` EntityDef is incremented to `1.2`.)

## Adds supertype to AtlasGlossaryCategory

As above for `AtlasGlossary`.

(The version of the `AtlasGlossaryCategory` EntityDef is incremented to `1.2`.)

## Adds supertype to AtlasGlossaryTerm

As above for `AtlasGlossary`.

(The version of the `AtlasGlossaryTerm` EntityDef is incremented to `1.2`.)

## How the extensions work

The extensions themselves are part of the source code tree under `src/main/resources/ApacheAtlasNativeTypesPatch.json`,
and are automatically deployed into the Apache Atlas environment during the initialization of the connector --
specifically, when you call the:

```
POST http://localhost:8080/open-metadata/admin-services/users/{{user}}/servers/{{server}}/instance
```

API interface of the OMAG Server Platform that has been configured to connect to an Apache Atlas environment
(see [Getting Started](../README.md)).

Because the extensions are necessary for the connector to operate, if there are any errors or problems deploying the
extensions you should be notified of these during the initialization: you will most likely receive an error `500`
response and should consult the Egeria and Apache Atlas logs for further details.

----
License: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/),
Copyright Contributors to the ODPi Egeria project.
