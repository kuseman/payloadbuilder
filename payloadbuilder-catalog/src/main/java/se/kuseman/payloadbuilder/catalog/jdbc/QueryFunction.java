package se.kuseman.payloadbuilder.catalog.jdbc;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Query function. Sends an unprocessed SQL query to the server */
class QueryFunction extends TableFunctionInfo
{
    QueryFunction(Catalog catalog)
    {
        super(catalog, "query");
    }

    @Override
    public String getDescription()
    {
        return "Send an unprocessed query to the database server " + System.lineSeparator()
               + "ie. select * from jdbc#query('select * from table') "
               + System.lineSeparator()
               + System.lineSeparator()
               + "A second argument (List) can be provided to make a prepared statement:  "
               + System.lineSeparator()
               + "select * from jdbc#query('select * from table where column = ?', listOf(10))";
    }

    @SuppressWarnings("unchecked")
    @Override
    public TupleIterator execute(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments, IDatasourceOptions options)
    {
        ValueVector queryVector = eval(context, arguments.get(0));
        if (queryVector.isNull(0))
        {
            return TupleIterator.EMPTY;
        }

        final String query = queryVector.getString(0)
                .toString();
        List<Object> parameters = null;
        if (arguments.size() >= 2)
        {
            Object obj = eval(context, arguments.get(1)).valueAsObject(0);
            if (!(obj instanceof List))
            {
                throw new IllegalArgumentException("Expected a list of objects second argument to " + getName() + " but got: " + obj);
            }
            parameters = (List<Object>) obj;
        }
        return JdbcDatasource.getIterator((JdbcCatalog) getCatalog(), context, catalogAlias, query, parameters, options.getBatchSize(context));
    }
}
