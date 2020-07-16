package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.QuerySession;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.Expression;
import com.viskan.payloadbuilder.parser.QueryParser;
import com.viskan.payloadbuilder.parser.TableOption;

import static java.util.Collections.emptyIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;

/** Base class of {@link OperatorBuilder} tests. */
public class AOperatorTest extends Assert
{
    protected final QueryParser parser = new QueryParser();
    protected final QuerySession session = new QuerySession(new CatalogRegistry());

    protected Expression e(String expression)
    {
        return parser.parseExpression(expression);
    }
    
    protected Operator op(final Function<ExecutionContext, Iterator<Row>> it)
    {
        return new Operator()
        {
            
            @Override
            public Iterator<Row> open(ExecutionContext context)
            {
                return it.apply(context);
            }
            
            @Override
            public int getNodeId()
            {
                return 0;
            }
        };
    }

    protected QueryResult getQueryResult(String query)
    {
        List<Operator> tableOperators = new ArrayList<>();
        MutableObject<TableAlias> mAlias = new MutableObject<>();
        Catalog c = new Catalog("Test")
        {
            @Override
            public Operator getScanOperator(QuerySession session, int nodeId, String catalogAlias, TableAlias alias, List<TableOption> tableOptions)
            {
                if (mAlias.getValue() == null)
                {
                    mAlias.setValue(alias);
                }
                Operator op = new Operator()
                {
                    @Override
                    public int getNodeId()
                    {
                        return 0;
                    }
                    
                    @Override
                    public Iterator<Row> open(ExecutionContext context)
                    {
                        return emptyIterator();
                    }

                    @Override
                    public String toString()
                    {
                        return String.format("%s (%d)", alias.getTable().toString(), nodeId);
                    }
                };

                tableOperators.add(op);
                return op;
            }
        };
        session.getCatalogRegistry().registerCatalog("c", c);
        session.setDefaultCatalog("c");
        Pair<Operator, Projection> pair = OperatorBuilder.create(session, parser.parseSelect(query));

        QueryResult result = new QueryResult();

        if (mAlias.getValue() != null)
        {
            TableAlias alias = mAlias.getValue();
            while (alias.getParent() != null)
            {
                alias = alias.getParent();
            }
            result.alias = alias;
        }
        result.operator = pair.getLeft();
        result.projection = pair.getRight();
        result.tableOperators = tableOperators;
        return result;
    }

    static class QueryResult
    {
        List<Operator> tableOperators;

        Operator operator;
        Projection projection;
        TableAlias alias;
    }
}
