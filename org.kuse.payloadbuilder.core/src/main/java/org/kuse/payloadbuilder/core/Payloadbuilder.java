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
     *   - Show functions should take an optional catalog (es#functions)
     *   - Extensibility
     *      Override parts of query and/remove/alter
     *   - Insert into temptable
     *      Needed for campaigns etc. Eventual stockhouse
     *   - Add more builtin functions
     *      - Make sure everything from old payloadbuilder is supported
     *   - Top operator for populating table source
     *      Top should be applied PER parent row and not as a stream as whole
     *   - Analyze select
     *      Perform select and measure stuff like catalog times, execution count
     *      Let catalogs provide data, like bytes fetched etc.
     *   - Long running queries not returning any rows, aren't ending when aborted
     *      Need to inside operators check for abort in ExecutionContext
     *   - Join hint (HashJoin, hash_inner = true)
     *   - Catalog   
     *      Send in Ex
     *   - Index types
     *      Full (all columns must be present)
     *      Random (one or more random columns)
     *      Left (one or more sequential columns from left)
     *   - Write SQL server catalog
     *   - UNION
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
     *   - Extract searchUrl/scrollUrl to a common place and add logic to it
     *      _index,_type_,_id,_parent cols shouldn't be fetched if not needed, check index/table alias
     *   - Add functions 
     *        search(index, body)
     *          - Search index with body (index empty/null search all)
     *        searchTemplate(index, template, params)
     *          - Search index with template (index empty/null search all)
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
