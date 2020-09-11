/**
 *
 *  Copyright (c) Marcus Henriksson <kuseman80@gmail.com>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.kuse.payloadbuilder.core;

import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.parser.QueryParser;
import org.kuse.payloadbuilder.core.parser.QueryStatement;

/** Main */
public class Payloadbuilder
{
    private static final QueryParser PARSER = new QueryParser();

    /*
     *
     *
     *
     * TODO:
     * BACKEND
     *   - TableAlias hierarchy isn't working correctly when having sub query and navigating
     *     to siblings
     *   - PredicateAnalyzer
     *       "@timestamp" > cast('2020-08-09T07:00:09.086', datetime)
     *         This one is UNDEFINED, and shouldn't, it's because of function arguments has a QRE which is a
     *         fake QRE only used for toString of Qname to determine datatype
     *       QualifiedVisitor
     *         Clean up code. Make a better method for finding an alias from a TableAlias, code is a mess now
     *   - QualifiedReferenceExpression
     *      Cache lookup path
     *   - Return a compiled query that clients can cache and reuse
     *      This would be good for performance because parse/operator build will be skipped.
     *   - Extensibility
     *      Override parts of query and/remove/alter
     *      Merge two queries?
     *      Hook to process query at AST level
     *   - Thread up batch hash join
     *      Each batch could be sent to a executor. Parallelism could be provided to set concurrency level
     *   - Insert into temptable
     *      Needed for campaigns etc. Eventual stockhouse
     *      WITH cache option
     *        Cache key: used variable names + statement id + all used catalog properties
     *          Ie. @country_id + 2 + { "es": { "endpoint": "....", "index": "...." } }
     *        Caching, framework or client ?
     *        Cache eviction
     *   - Catalog supported top operator
     *   - Top operator for populating table source
     *      Top should be applied PER parent row and not as a stream as whole
     *   - Analyze select
     *      Perform select and measure stuff like catalog times, execution count
     *      Let catalogs provide data, like bytes fetched etc.
     *   - Join hint (HashJoin, hash_inner = true)
     *   - Add support for catalog to provide stats for tables
     *      - Estimated number of rows
     *   - Catalog
     *      Send in ExecutionContext when fetching indcies, to let catalog store information
     *      that will be used later on then operators are created
     *   - Index types
     *      Full (all columns must be present)
     *      Random (one or more random columns)
     *      Left (one or more sequential columns from left)
     *   - Write SQL server catalog
     *   - Insert / update /delete
     *   - Operators
     *      Union
     *      Except
     *      Intersect
     *      Semi join (where exists)
     *      Anti join (where not exists)
     *   - BatchMergeJoin
     *      Fix broken implementation
     *   - Caching parsed query
     *   - Codegen of expressions
     *   - Child first class loader
     *      If there is conflicts between deps in extensions, we need
     *      to change to a child first class loader so extensions can embed their own versions
     *      This is a security issue but think it's no problem.
     * ESCatalog
     *   - All non_analyzed fields are potential index-columns
     *   - Fetch ES version to apply correct URLS in all places (7.5.2 doesn't have tables, add a dummy table data or similar)
     * EtmArticleCategoryESCatalog
     *   - UI: Split endpoint/index selection dropdowns
     *   - Refactor and reuse stuff from ESCatalog if possible
     *
     * EDITOR:
     *   - Always start with a blank file
     *   - Open multi files
     *   - Bug in service when columns are gathered
     *     select array(*)  <-- should yield a no column name but gets the last column from the array values
     *     from article_category
     */

    private Payloadbuilder()
    {
    }

    /** Perform query with provided query string */
    public static QueryResult query(String queryString)
    {
        return query(new QuerySession(new CatalogRegistry()), queryString);
    }

    /** Perform query with provided session and query string */
    public static QueryResult query(QuerySession session, String queryString)
    {
        QueryStatement query = PARSER.parseQuery(queryString);
        return new QueryResultImpl(session, query);
    }
}
