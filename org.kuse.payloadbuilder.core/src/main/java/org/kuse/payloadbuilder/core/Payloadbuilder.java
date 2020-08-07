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
     *   - Try to rewrite PredicateAnalyzer
     *      Make it easier to work with
     *      Add type in pair to be able to easier add range queries etc.
     *      See if the includeEmptyAlias thing can be removed
     *      Now it's not possible to use a "NOT a.flag" for example, which would
     *      be a fully working Elastic predicate
     *   - Pushdown filter is not grouped when one or the other is written without alias
     *      Could not re-produce, might been with a join, leaving it here
     *   - QualifiedReferenceExpression
     *      Cache lookup path
     *   - Extensibility
     *      Override parts of query and/remove/alter
     *   - Rewrite OperatorBuilder
     *      visit(PopulateTableSource ...)
     *        Fix so push down predicates is set in context for catalog usage
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
