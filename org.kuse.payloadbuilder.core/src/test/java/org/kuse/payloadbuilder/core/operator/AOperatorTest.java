package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.Catalog.TablePredicate;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QueryParser;
import org.kuse.payloadbuilder.core.parser.SortItem;

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
        return getQueryResult(query, null, null);
    }
    
    protected QueryResult getQueryResult(String query, Consumer<TablePredicate> predicateConsumer, Consumer<List<SortItem>> sortItemsConsumer)
    {
        List<Operator> tableOperators = new ArrayList<>();
        MutableObject<TableAlias> mAlias = new MutableObject<>();
        Catalog c = new Catalog("Test")
        {
            @Override
            public Operator getScanOperator(OperatorData data)
            {
                if (predicateConsumer != null)
                {
                    predicateConsumer.accept(data.getPredicate());
                }
                
                if (sortItemsConsumer != null)
                {
                    sortItemsConsumer.accept(data.getSortItems());
                }
                
                if (mAlias.getValue() == null)
                {
                    mAlias.setValue(data.getTableAlias());
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
                        return String.format("%s (%d)", data.getTableAlias().getTable().toString(), data.getNodeId());
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