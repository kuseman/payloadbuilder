package se.kuseman.payloadbuilder.catalog.jdbc;

import static java.util.Collections.emptyList;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.catalog.jdbc.JdbcCatalog.ColumnOption;
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
               + "select * from jdbc#query('select * from table where column = ?', array(10))";
    }

    @Override
    public TupleIterator execute(IExecutionContext context, String catalogAlias, List<IExpression> arguments, FunctionData data)
    {
        ValueVector queryVector = arguments.get(0)
                .eval(context);
        if (queryVector.isNull(0))
        {
            return TupleIterator.EMPTY;
        }

        final String query = queryVector.getString(0)
                .toString();
        List<Object> parameters = emptyList();
        if (arguments.size() >= 2)
        {
            ValueVector obj = arguments.get(1)
                    .eval(context);
            obj = obj.isNull(0) ? null
                    : obj.getArray(0);
            if (obj != null)
            {
                parameters = obj.toList();
            }
        }

        SqlDialect dialect = DialectProvider.getDialect(context.getSession(), catalogAlias);

        return JdbcDatasource.getIterator(dialect, catalog, context, catalogAlias, query, parameters, context.getBatchSize(data.getOptions()), ColumnOption.extract(data.getOptions()));
    }
}
