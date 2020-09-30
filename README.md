[![Actions Status](https://github.com/kuseman/payloadbuilder/workflows/Java%20CI%20with%20Maven/badge.svg)](https://github.com/kuseman/payloadbuilder/actions)

# Payloadbuilder

SQL query engine with plugable catalogs (data sources)

## Table of Contents

* [About the Project](#about-the-project)
  * [Catalogs](#catalogs)
  * [Built With](#built-with)
* [Usage](#usage)
* [Getting Started](#getting-started)
  * [Editor](#editor)
* [Roadmap](#roadmap)
* [Contributing](#contributing)
* [License](#license)

## About the project

Query engine with SQL support, easy to extend with catalogs to allow for joining data sets from completely different data sources.
A simple analyzer that builds an operator tree from provided query for data retrieval.

Supports:

* JOIN (INNER, LEFT)
* APPLY (CROSS, OUTER)
* TVF's
* Scalar functions
* Key lookup Index
* Multiple statements
* Control-flow statements

### Catalogs

Catalogs is the extension point where external data is provided.
A catalog implementation can provide scalar functions, table valued functions,
data operators for tables.

### Built With

* JDK 8
* Maven

## Usage

* mvn install
* Add Reference 
  * **org.kuse.payloadbuilder.core/target/org.kuse.payloadbuilder.core-{version}.jar**
  * **org.kuse.payloadbuilder.catalog/target/org.kuse.payloadbuilder.catalog-{version}.jar**
    * Contains bundeled Catalog implementations
  * Release to Central is coming soon

## Getting Started

Simple query using multiple catalogs:

```java
        QuerySession session = new QuerySession(new CatalogRegistry());
        session.setPrintStream(System.out);
        session.getCatalogRegistry().registerCatalog("es", new ESCatalog());
        session.getCatalogRegistry().registerCatalog("fs", new FileSystemCatalog());
        session.setDefaultCatalog("es");

        session.setCatalogProperty("es", "endpoint", "http://localhost:9200");
        session.setCatalogProperty("es", "index", "myindex");

        String query = "select top 10 " +
            "        d.fileName " + 
            ",       f.size " + 
            ",       f.lastModifiedTime " + 
            "from es#_doc d " + 
            "cross apply fs#file(concat('/path/to/files/', d.fileName)) f " + 
            "where d._docType = 'pdfFiles'";

        QueryResult queryResult = Payloadbuilder.query(session, query);
        JsonStringWriter writer = new JsonStringWriter();
        while (queryResult.hasMoreResults())
        {
          queryResult.writeResult(writer);
          System.out.println(writer.getAndReset());
        }
```

### Editor

Standalone Query Editor written in Swing

[Editor](https://github.com/kuseman/payloadbuilder/tree/master/org.kuse.payloadbuilder.editor)
![Editor](/documentation/editor.png?raw=true "Optional Title")

## Raodmap

Alot :)
* Documentation
* Insert/Update/Delete support
* Merge join operator
* More catalog implementations (JDBC, Mongo, Redis etc.)

## Contributing

Patches, discussions about features, changes etc. welcome. 

## License

Distributed under the Apache License Version 2.0. See `LICENSE` for more information.

