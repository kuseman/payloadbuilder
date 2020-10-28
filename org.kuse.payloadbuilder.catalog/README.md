# Payloadbuilder Catalogs

## Table of Contents

* [Filesystem](#filesystem)
* [Elasticsearch](#elasticsearch)
* [Jdbc](#jdbc)

## Filesystem

Simple catalog that provides file system access.
Does not provide any tables.

#### Catalog properties
N/A

#### Qualified name
N/A

#### Functions

| Name          | Type          | Description                                  | Arguments                             | Example                                                     |
|:------------- |:--------------|:---------------------------------------------|:--------------------------------------|-------------------------------------------------------------|
| list          | Table         | Lists files in provided path                 | path (String) [, recursive (Boolean)] | select *<br/>from fs#list('/tmp', true)                         |
| file          | Table         | Opens a single file with provided path       | path (String)                         | select *<br/>from table t<br/>cross apply fs#file(t.file) f |
| contents      | Scalar        | Returns contents of provided path as String  | path (String)                         | select fs#contents('/tmp/file')                             |

## Elasticsearch

Catalog that provides access to elasticsearch.

#### Catalog properties

| Name          |  Description              |
|:------------- |:--------------------------|
| endpoint      | Endpoint to elasticsearch |
| index         | Index in elasticsearch    |
| type          | Type in index             |


#### Qualified name

| Parts | Description                                                           | Example                                                                                       | 
|-------|-----------------------------------------------------------------------|-----------------------------------------------------------------------------------------------|
| 1     | Type<br/> Expected catalog properties: <b>endpoint</b>, <b>index</b>  | use es.endpont = 'http://localhost:9200'<br/>use.es.index = 'myIndex'<br/> select * from type |
| 2     | Index, Type<br/> Expected catalog properties: <b>endpoint</b>         | use es.endpont = 'http://localhost:9200'<br/> select * from myIndex.type                      |
| 3     | Endpoint, Index, Type                                                 | select * from "http://localhost:9200".myIndex.type                                            |
| ...   | Not supported                                                         |                                                                                               |

NOTE! For ES versions where type is not longer present the qualified name **_doc** is used. 

#### Functions

| Name            | Type          | Description                                        | Arguments                     | Example |
|:----------------|:--------------|:---------------------------------------------------|:------------------------------|---------|
| mustachecompile | Scalar        | Compiles provided mustasch template with arguments | template (Sting), model (Map) |         |
| search          | Table         | Queries Elasticsearch with a nativ query           | **Named arguments:**<br/> endpoint (String) (Optional if provided in catalog properties <br/>index (String) (Optional if provided in catalog properties <br/>type (String) <br/>body (String) Mutual exclusive with template <br/>template (String) Mutual exclusive with body <br/>scroll (Boolean)<br/>params (Map) Model provided to template| use es.endpoint = 'http://localhost:9200'<br/>use es.index='myIndex'<br/><br/>select *<br/>from es#search(<br/>body: '{ "filter": { "match_all": {} }',<br/>scroll: true<br/>) |

## Jdbc

Catalog that provides access to Jdbc compliant systems

#### Qualified name
Fully qualified name is sent to Jdbc as is

#### Catalog properties

| Name             | Description               |
|:-----------------|:--------------------------|
| url              | Jdbc connection url       |
| username         | Username for connection   |
| password         | Password for connection   |

#### Functions

| Name            | Type          | Description                                        | Arguments                     | Example |
|:----------------|:--------------|:---------------------------------------------------|:------------------------------|---------|
| query           | Table         | Queries Jdbc with a nativ query                    | query (String)<br/>parameters (List) Optional parameters for the prepared statement in query | use jdbc.url = 'jdbc://........'<br/>use jdbc.username = 'user'<br/>use jdbc.password = 'pass'<br/><br/>select *<br/>from jdbc#query('select * from table where col1 = ?', listOf(1337)) |
