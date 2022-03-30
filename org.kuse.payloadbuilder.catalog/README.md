
# Payloadbuilder Catalogs

## Table of Contents

* [System](#system)
* [Filesystem](#filesystem)
* [Elasticsearch](#elasticsearch)
* [Jdbc](#jdbc)

## System

Catalog that add support for various system tables

- catalogs (List registered catalogs in session)
- functions (List built in functions)
- tables (List temp tables in session)
- caches (List sessions caches)
- cachekeys (List sessions cache keys)

This table also redirects table queries to other registered catalogs (if they implement it).
For example:
```sql
-- List various system tables for catalog registed with alias 'es'

select *
from sys#es.tables

select *
from sys#es.columns

select *
from sys#es.indices
```

These table queries can be implemented by catalogs via Catalog#getSystemOperator
The system catalog is automatically registered in the session with alias `sys` 

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
| endpoint      | (string) Endpoint to elasticsearch |
| index         | (string) Index in elasticsearch    |
| type          | (string) Type in index             |
| cache.mappings.ttl | (integer) Cache TTL for mappings. To avoid excessive queries against ES for mappings etc. that is needed
to properly utilize indices, predicate push downs, order by's etc. that information is put to PLB's CUSTOM cache. Defaults to 60 minutes. |


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
|match|Scalar|Function that utilizes ES match operator as a predicate. NOTE! Only applicable in query predicates for ES catalog tables.|matchFields (Qualified name or String with comma separated field names. If multiple fields are used then a multi_match query is used.)<br/>field(s) (String), query (String)|select *<br/>from es#_doc<br/>where match(name, 'some phrase')<br/><br/>select *<br/>from es#_doc<br/>where es#match('name,message', 'some phrase')|
|query|Scalar|Function that utilizes ES query_string operator as a predicate. NOTE! Only applicable in query predicates for ES catalog tables.|query (String)<br/>|select *<br/>from es#_doc<br/>where es#query('type:log AND severity:200')|
|cat|Table|Function that exposes elastic search cat-api as a table|optional endpoint (String), catspec (String)|select * from es#cat('nodes?s=name:desc')|

#### Misc

LIKE operator is pushed down as a Elastic WILDCARD query

## Jdbc

Catalog that provides access to Jdbc compliant systems

#### Qualified name
Fully qualified name is sent to Jdbc as is

#### Catalog properties

| Name             | Description               |
|:-----------------|:--------------------------|
| driverclassname  | Jdbc driver class name    |
| url              | Jdbc connection url       |
| username         | Username for connection   |
| password         | Password for connection   |

#### Functions

| Name            | Type          | Description                                        | Arguments                     | Example |
|:----------------|:--------------|:---------------------------------------------------|:------------------------------|---------|
| query           | Table         | Queries Jdbc with a nativ query                    | query (String)<br/>parameters (List) Optional parameters for the prepared statement in query | use jdbc.url = 'jdbc://........'<br/>use jdbc.username = 'user'<br/>use jdbc.password = 'pass'<br/><br/>select *<br/>from jdbc#query('select * from table where col1 = ?', listOf(1337)) |
