package org.kuse.payloadbuilder.catalog.es;

import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/**
 * Match function that is applicable in query predicates that uses ES query_string operator on free text fields
 *
 * <pre>
 * match(fields,
 * </pre>
 */
class QueryFunction extends ScalarFunctionInfo
{
    QueryFunction(Catalog catalog)
    {
        super(catalog, "query");
    }

    @Override
    public String getDescription()
    {
        return "Function that is used in Elastic search query context (predicates) and utilizes Elastic 'query_string' query. " + System.lineSeparator()
            + "ex. query(<query expression>) "  + System.lineSeparator()
            + "See: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html";
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        throw new IllegalArgumentException("'query' cannot be used in non Elastic query context.");
    }
}
