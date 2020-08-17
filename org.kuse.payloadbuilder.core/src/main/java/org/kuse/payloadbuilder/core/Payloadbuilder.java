package org.kuse.payloadbuilder.core;

import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.parser.QueryParser;
import org.kuse.payloadbuilder.core.parser.QueryStatement;

/** Main */
public class Payloadbuilder
{
    private static final QueryParser PARSER = new QueryParser();

    /*
     * TODO:
     * BACKEND
     *   - Pushdown is possible when LEFT JOIN on the inner table ONLY
     *       
     *   - WHERE count(a.aam) > 0 is pushed down which is wrong QRE with alias access cannot be pushed
     *     - Push down can be made to populating where but not to table level
     *     - Fix in PredicateAnalyzer
     *   - PredicateAnalyzer
     *       "@timestamp" > cast('2020-08-09T07:00:09.086', datetime) 
     *         This one is UNDEFINED, and shouldn't, it's because of function arguments has a QRE which is a 
     *         fake QRE only used for toString of Qname to determine datatype
     *       Add types for NULL predicate IS NULL, IS NOT NULL, for easier building of predicates in Catalogs
     *   - QueryParser
     *     Build TableAlias at parse time and calculate path to destination for QRE at that time (might be problematic 
     *      for complex select items)
     *   - QualifiedReferenceExpression
     *      Cache lookup path
     *   - Return a compiled query that clients can cache and reuse
     *      This would be good for performance because parse/operator build will be skipped.
     *   - Extensibility
     *      Override parts of query and/remove/alter
     *      Merge two queries?
     *      Hook to process query at AST level
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
     *   - Long running queries not returning any rows, aren't ending when aborted
     *      Need to inside operators check for abort in ExecutionContext
     *   - Join hint (HashJoin, hash_inner = true)
     *   - Remove concept parameters (:params) and instead only use variables
     *     Then one can use expressions like "set @lang_id = isnull(@lang_id, 3)
     *     to fallback on missing input parameters
     *   - Catalog   
     *      Send in ExecutionContext when fetching indcies, to let catalog store information
     *      that will be used later on then operators are created
     *   - Index types
     *      Full (all columns must be present)
     *      Random (one or more random columns)
     *      Left (one or more sequential columns from left)
     *   - Write SQL server catalog
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
     *   - UI: Split endpoint/index selection dropdowns
     *   - All non_analyzed fields are potential index-columns
     *   - Add functions 
     *        search(index, body)
     *          - Search index with body (index empty/null search all)
     *        searchTemplate(index, template, params)
     *          - Search index with template (index empty/null search all)
     * EtmArticleCategoryESCatalog
     *   - UI: Split endpoint/index selection dropdowns
     *   - Refactor and reuse stuff from ESCatalog if possible
     *   - Implement predicate support for index-column
     *   
     * EDITOR:
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
