package se.kuseman.payloadbuilder.catalog.es;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

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
        super(catalog, "query", FunctionType.SCALAR);
    }

    @Override
    public String getDescription()
    {
        return "Function that is used in Elastic search query context (predicates) and utilizes Elastic 'query_string' query. " + System.lineSeparator()
               + "ex. query(<query expression>) "
               + System.lineSeparator()
               + "See: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html";
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        throw new IllegalArgumentException("'match' cannot be used in non Elastic query context.");
    }
}
