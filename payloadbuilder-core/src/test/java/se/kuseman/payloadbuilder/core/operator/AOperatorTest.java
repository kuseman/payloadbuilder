package se.kuseman.payloadbuilder.core.operator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.mutable.MutableObject;

import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.IAnalyzePair;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Operator.TupleIterator;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.core.operator.OperatorBuilder.BuildResult;
import se.kuseman.payloadbuilder.core.parser.AParserTest;
import se.kuseman.payloadbuilder.core.parser.QueryStatement;
import se.kuseman.payloadbuilder.core.parser.SelectStatement;

/** Base class of {@link OperatorBuilder} tests. */
public class AOperatorTest extends AParserTest
{
    protected Stream<Tuple> stream(Iterable<Tuple> iterable)
    {
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    protected Operator op(final Function<IExecutionContext, Iterator<Tuple>> it)
    {
        return op(it, null);
    }

    protected Operator op(final Function<IExecutionContext, Iterator<Tuple>> itFunc, Runnable closeAction)
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

    protected Operator op1(final Function<IExecutionContext, TupleIterator> it)
    {
        return op1(it, null);
    }

    protected Operator op1(final Function<IExecutionContext, TupleIterator> itFunc, Runnable closeAction)
    {
        return new Operator()
        {
            @Override
            public TupleIterator open(IExecutionContext context)
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

    protected QueryResult getQueryResult(String query, Consumer<List<IAnalyzePair>> predicateConsumer, Consumer<List<ISortItem>> sortItemsConsumer)
    {
        List<Operator> tableOperators = new ArrayList<>();
        MutableObject<TableAlias> operatorAlias = new MutableObject<>();
        List<TableAlias> aliases = new ArrayList<>();
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

                aliases.add(data.getTableAlias());

                if (operatorAlias.getValue() == null)
                {
                    operatorAlias.setValue(data.getTableAlias());
                }
                Operator op = new Operator()
                {
                    @Override
                    public int getNodeId()
                    {
                        return 0;
                    }

                    @Override
                    public TupleIterator open(IExecutionContext context)
                    {
                        return TupleIterator.EMPTY;
                    }

                    @Override
                    public String toString()
                    {
                        return String.format("%s (%d)", data.getTableAlias()
                                .getTable()
                                .toString(), data.getNodeId());
                    }
                };

                tableOperators.add(op);
                return op;
            }
        };
        session.getCatalogRegistry()
                .registerCatalog("c", c);
        session.setDefaultCatalogAlias("c");

        // Assume the last statement is the one we should create operator for
        QueryStatement stms = q(query);
        SelectStatement select = (SelectStatement) stms.getStatements()
                .get(stms.getStatements()
                        .size() - 1);
        BuildResult buildResult = OperatorBuilder.create(session, select.getSelect());

        QueryResult result = new QueryResult();

        if (operatorAlias.getValue() != null)
        {
            TableAlias alias = operatorAlias.getValue();
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

        return tuple != null ? tuple.getValue(columnOrdinal)
                : null;
    }

    protected Object getValue(Tuple t, int tupleOrdinal, String column)
    {
        Tuple tuple = t;
        if (tupleOrdinal != -1)
        {
            tuple = t.getTuple(tupleOrdinal);
        }

        return tuple != null ? tuple.getValue(tuple.getColumnOrdinal(column))
                : null;
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
