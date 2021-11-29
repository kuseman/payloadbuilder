package org.kuse.payloadbuilder.core.operator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.mutable.MutableObject;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.operator.Operator.TupleIterator;
import org.kuse.payloadbuilder.core.operator.OperatorBuilder.BuildResult;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair;
import org.kuse.payloadbuilder.core.parser.AParserTest;
import org.kuse.payloadbuilder.core.parser.SortItem;

/** Base class of {@link OperatorBuilder} tests. */
public class AOperatorTest extends AParserTest
{
    protected Stream<Tuple> stream(Iterable<Tuple> iterable)
    {
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    protected Operator op1(final Function<ExecutionContext, TupleIterator> it)
    {
        return op1(it, null);
    }

    protected Operator op(final Function<ExecutionContext, Iterator<Tuple>> it)
    {
        return op(it, null);
    }

    protected Operator op(final Function<ExecutionContext, Iterator<Tuple>> itFunc, Runnable closeAction)
    {
        return op1(ctx -> new TupleIterator()
        {
            Iterator<Tuple> it = itFunc.apply(ctx);
            @Override
            public Tuple next()
            {
                return it.next();
            }

            @Override
            public boolean hasNext()
            {
                return it.hasNext();
            }
        }, closeAction);
    }

    protected Operator op1(final Function<ExecutionContext, TupleIterator> itFunc, Runnable closeAction)
    {
        return new Operator()
        {
            @Override
            public TupleIterator open(ExecutionContext context)
            {
                final TupleIterator it = itFunc.apply(context);
                return new TupleIterator()
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
                    public TupleIterator open(ExecutionContext context)
                    {
                        return TupleIterator.EMPTY;
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
        BuildResult buildResult = OperatorBuilder.create(session, s(query));

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
        result.operator = buildResult.getOperator();
        result.projection = buildResult.getProjection();
        result.tableOperators = tableOperators;
        return result;
    }

    protected Object getValue(Tuple t, int tupleOrdinal, int columnOrdinal)
    {
        Tuple tuple = t;
        if (tupleOrdinal != -1)
        {
            tuple = t.getTuple(tupleOrdinal);
        }

        return tuple != null ? tuple .getValue(columnOrdinal) : null;
    }

    protected Object getValue(Tuple t, int tupleOrdinal, String column)
    {
        Tuple tuple = t;
        if (tupleOrdinal != -1)
        {
            tuple = t.getTuple(tupleOrdinal);
        }

        return tuple != null ? tuple.getValue(tuple.getColumnOrdinal(column)) : null;
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
