package org.kuse.payloadbuilder.catalog.jdbc;

import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.parser.Expression;

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
            + "ie. select * from jdbc#query('select * from table') " + System.lineSeparator() + System.lineSeparator()
            + "A second argument (List) can be provided to make a prepared statement:  " + System.lineSeparator()
            + "select * from jdbc#query('select * from table where column = ?', listOf(10))";
    }

    @SuppressWarnings("unchecked")
    @Override
    public RowIterator open(ExecutionContext context, String catalogAlias, TableAlias tableAlias, List<Expression> arguments)
    {
        final String query = (String) arguments.get(0).eval(context);
        List<Object> parameters = null;
        if (arguments.size() >= 2)
        {
            Object obj = arguments.get(1).eval(context);
            if (!(obj instanceof List))
            {
                throw new IllegalArgumentException("Expected a list of objects second argument to " + getName() + " but got: " + obj);
            }
            parameters = (List<Object>) obj;
        }
        return JdbcOperator.getIterator((JdbcCatalog) getCatalog(), context, catalogAlias, tableAlias, query, parameters);
    }
}
