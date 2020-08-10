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
