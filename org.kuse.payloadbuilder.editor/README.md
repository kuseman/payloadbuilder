# Payloadbuilder Editor

## Table of Contents

* [Usage](#usage)
* [Developing](#developing)

## Usage

* [Build](https://github.com/kuseman/payloadbuilder/blob/master/README.md#usage)
* Create a **config.json** file with Catalog extensions ([Definition](https://github.com/kuseman/payloadbuilder/blob/master/org.kuse.payloadbuilder.editor/src/main/java/org/kuse/payloadbuilder/editor/Config.java))
```json
{
  "catalogs": [
    {
      "jar": "org.kuse.payloadbuilder.catalog/target/org.kuse.payloadbuilder.catalog-0.0.1-SNAPSHOT.jar",
      "className": "org.kuse.payloadbuilder.catalog.es.ESCatalogExtension",
      "config": {
        "endpoints": [
          "http://es-instance1:9200",
          "http://es-instance2:9200"
        ]
      }
    },
    {
      "jar": "org.kuse.payloadbuilder.catalog/target/org.kuse.payloadbuilder.catalog-0.0.1-SNAPSHOT.jar",
      "className": "org.kuse.payloadbuilder.catalog.fs.FilesystemCatalogExtension",
      "config": {
        
      }
    }
  ]
}
```
* java -jar org.kuse.payloadbuilder.editor/org.kuse.payloadbuilder.editor-0.0.1-SNAPSHOT-jar-with-dependencies.jar

## Developing

Adding UI extension for Catalog implementations is a matter of implementing [ICatalogExtension](https://github.com/kuseman/payloadbuilder/blob/master/org.kuse.payloadbuilder.editor/src/main/java/org/kuse/payloadbuilder/editor/ICatalogExtension.java)
and referencing that class in **config.json**

Note! Extensions are expected to be fat JARS with provided dependencies.
