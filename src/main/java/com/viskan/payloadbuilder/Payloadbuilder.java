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
     *   - Index support on FROM clause
     *      id = <...>
     *      id IN (1,2,3,)
     *      Wrap index operator with a outer values provider  
     *   - Order by
     *      Implement
     *   - Extensibility
     *      Override parts of query and/remove/alter
     *   - Sources support (art_id/attr1_id lists)
     *      Built in operator ?  
     *   - Add more builtin functions
     *      - lowercase
     *      - ltrim
     *      - rtrim
     *      - leftpad
     *      - dateadd
     *      - Make sure everything from old payloadbuilder is supported
     *   - Describe support
     *      Table (fetch one row and show available columns)
     *      Function (show metadata, arguments, documentation)
     *      Select (build query plan and show in result set (indented tree))
     *        Add support for catalogs to provide info in a object like fashion to show in a column
     *   - Analyze select
     *      Perform select and measure stuff like catalog times, execution count
     *      Let catalogs provide data, like bytes fetched etc.
     *   - Show parameters/variables
     *   - Add support for project alias.*
     *   - Write SQL server catalog
     *   - BatchMergeJoin
     *      Fix broken implementation
     *   - Caching parsed query
     *   - Codegen of expressions
     * 
     * UI
     *   - Catalog config
     *      - Model based
     *      - Reactive components in first view (session index/database etc.)
     *      - Config dialog
     *   - Adding/removing catalog extensions
     *   - Load/save catalog config
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
