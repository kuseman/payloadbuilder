package com.viskan.payloadbuilder;

import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.parser.QueryParser;
import com.viskan.payloadbuilder.parser.QueryStatement;

/** Main */
public class Payloadbuilder
{
    private static final QueryParser PARSER = new QueryParser();

    /*
     * TODO:
     * BACKEND
     *   - QualifiedReferenceExpression
     *      Cache lookup path
     *      Traverse up through parents
     *   - Correlated queries support
     *   - Extensibility
     *      Override parts of query and/remove/alter
     *   - Sources support (art_id/attr1_id lists)
     *      Built in operator ?  
     *   - Rewrite OperatorBuilder#visit(PopulateTableSource ...)
     *      Fix so push down predicates is set in context for catalog usage
     *   - Insert into temptable
     *      Needed for campaigns etc. Eventual stockhouse
     *   - Add more builtin functions
     *      - lowercase
     *      - ltrim
     *      - rtrim
     *      - leftpad
     *      - dateadd
     *      - Make sure everything from old payloadbuilder is supported
     *   - Try to rewrite PredicateAnalyzer
     *      Make it easier to work with
     *      Add type in pair to be able to easier add range queries etc.
     *      See if the includeEmptyAlias thing can be removed
     *   - Describe support
     *      Catalog (show functions, tables, description of catalog)
     *      Function (show metadata, arguments, documentation)
     *   - Analyze select
     *      Perform select and measure stuff like catalog times, execution count
     *      Let catalogs provide data, like bytes fetched etc.
     *   - Add support for project alias.*, *
     *   - Write SQL server catalog
     *   - BatchMergeJoin
     *      Fix broken implementation
     *   - Caching parsed query
     *   - Codegen of expressions
     *   - DeferenceExpression
     *      Implement
     * 
     * UI
     *   - Move UI to own bundle

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
