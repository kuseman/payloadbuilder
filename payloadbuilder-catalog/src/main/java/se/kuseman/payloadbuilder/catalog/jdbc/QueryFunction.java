package se.kuseman.payloadbuilder.catalog.jdbc;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.catalog.jdbc.dialect.DialectProvider;
import se.kuseman.payloadbuilder.catalog.jdbc.dialect.SqlDialect;

/** Query function. Sends an unprocessed SQL query to the server */
class QueryFunction extends TableFunctionInfo
{
    private final JdbcCatalog catalog;

    QueryFunction(JdbcCatalog catalog)
    {
        super("query");
        this.catalog = catalog;
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
    public TupleIterator execute(IExecutionContext context, String catalogAlias, List<IExpression> arguments, List<Option> options)
    {
        ValueVector queryVector = arguments.get(0)
                .eval(context);
        if (queryVector.isNull(0))
        {
            return TupleIterator.EMPTY;
        }

        final String query = queryVector.getString(0)
                .toString();
        List<Object> parameters = null;
        if (arguments.size() >= 2)
        {
            Object obj = arguments.get(1)
                    .eval(context)
                    .valueAsObject(0);
            if (!(obj instanceof List))
            {
                throw new IllegalArgumentException("Expected a list of objects second argument to " + getName() + " but got: " + obj);
            }
            parameters = (List<Object>) obj;
        }

        SqlDialect dialect = DialectProvider.getDialect(context.getSession(), catalogAlias);

        return JdbcDatasource.getIterator(dialect, catalog, context, catalogAlias, null, query, parameters, context.getBatchSize(options));
    }
}
