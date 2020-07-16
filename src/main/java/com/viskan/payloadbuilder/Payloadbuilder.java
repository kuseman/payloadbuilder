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
     *      Make traversal of maps work correctly
     *   - DeferenceExpression
     *      Implement
     *   - Correlated queries support
     *   - Extensibility
     *      Override parts of query and/remove/alter
     *   - Sources support (art_id/attr1_id lists)
     *      Built in operator ?  
     *   - Catalog supported predicates
     *      Catalog function for supported predicate fields
     *      Extract those fields from predicate and send into create operator method
     *      Add the rest as filter as usual
     *   - Insert into temptable
     *      Needed for campaigns etc. Eventual stockhouse
     *   - Add more builtin functions
     *      - lowercase
     *      - ltrim
     *      - rtrim
     *      - leftpad
     *      - dateadd
     *      - Make sure everything from old payloadbuilder is supported
     *   - Describe support
     *      Function (show metadata, arguments, documentation)
     *   - Analyze select
     *      Perform select and measure stuff like catalog times, execution count
     *      Let catalogs provide data, like bytes fetched etc.
     *   - Add support for project alias.*
     *   - Write SQL server catalog
     *   - BatchMergeJoin
     *      Fix broken implementation
     *   - Caching parsed query
     *   - Codegen of expressions
     * 
     * UI
     *   - Parameters toolbar button
     *      - Find out parameters by traversing query
     *      - Populate key/value editor (JSON values)
     *      - Store current params in QueryFileModel/session
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
