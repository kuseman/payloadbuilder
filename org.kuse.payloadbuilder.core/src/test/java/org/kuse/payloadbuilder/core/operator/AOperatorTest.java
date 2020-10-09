package org.kuse.payloadbuilder.core.operator;

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
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair;
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
        return parser.parseExpression(session.getCatalogRegistry(), expression);
    }

    protected Operator op(final Function<ExecutionContext, Iterator<Tuple>> it)
    {
        return op(it, null);
    }

    protected Operator op(final Function<ExecutionContext, Iterator<Tuple>> itFunc, Runnable closeAction)
    {
        return new Operator()
        {
            @Override
            public RowIterator open(ExecutionContext context)
            {
                final Iterator<Tuple> it = itFunc.apply(context);
                return new RowIterator()
                {
                    @Override
                    public Tuple next()
                    {
                        return it.next();
                    }

                    @Override
                    public void close()
                    {
                        if (closeAction != null)
                        {
                            closeAction.run();
                        }
                    }

                    @Override
                    public boolean hasNext()
                    {
                        return it.hasNext();
                    }
                };
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

    protected QueryResult getQueryResult(String query, Consumer<List<AnalyzePair>> predicateConsumer, Consumer<List<SortItem>> sortItemsConsumer)
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
                    predicateConsumer.accept(data.getPredicatePairs());
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
                    public RowIterator open(ExecutionContext context)
                    {
                        return RowIterator.EMPTY;
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
        session.getCatalogRegistry().setDefaultCatalog("c");
        Pair<Operator, Projection> pair = OperatorBuilder.create(session, parser.parseSelect(session.getCatalogRegistry(), query));

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

    /** Query result */
    static class QueryResult
    {
        List<Operator> tableOperators;

        Operator operator;
        Projection projection;
        TableAlias alias;
    }
}
