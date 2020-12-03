package org.kuse.payloadbuilder.catalog.es;

import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
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
        return "Function that is used in Elastic search query context (predicates) and utilizes Elastic match operator. " + System.lineSeparator()
            + "ex. match(<fields expression>, <match expression>, [<options expression>)";
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        throw new IllegalArgumentException("'query' cannot be used in non Elastic query context.");
    }
}
