package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.utils.MapUtils;

/** Operator that computes columns from expressions */
class ComputedColumnsOperator extends AOperator
{
    private final Operator operator;
    private final List<Expression> computedExpressions;
    private final List<String> columns;

    ComputedColumnsOperator(int nodeId, Operator operator, List<String> columns, List<Expression> computedExpressions)
    {
        super(nodeId);
        this.operator = requireNonNull(operator, "operator");
        this.columns = requireNonNull(columns, "columns");
        this.computedExpressions = requireNonNull(computedExpressions, "computedExpressions");
    }

    @Override
    public List<Operator> getChildOperators()
    {
        return singletonList(operator);
    }

    @Override
    public String getName()
    {
        return "Compute columns";
    }

    @Override
    public Map<String, Object> getDescribeProperties(ExecutionContext context)
    {
        int size = columns.size();
        return MapUtils.ofEntries(MapUtils.entry("Columns", IntStream.range(0, size).mapToObj(i -> String.format("%s: %s", columns.get(i), computedExpressions.get(i))).collect(joining())));
    }

    @Override
    public RowIterator open(ExecutionContext context)
    {
        final RowIterator it = operator.open(context);
        //CSOFF
        return new RowIterator()
        //CSON
        {
            @Override
            public Tuple next()
            {
                Tuple tuple = it.next();
                int size = computedExpressions.size();
                Object[] values = new Object[size];
                for (int i = 0; i < size; i++)
                {
                    context.setTuple(tuple);
                    values[i] = computedExpressions.get(i).eval(context);
                }

                return new ComputedTuple(tuple, values);
            }

            @Override
            public boolean hasNext()
            {
                return it.hasNext();
            }

            @Override
            public void close()
            {
                it.close();
            }
        };
    }

    @Override
    public int hashCode()
    {
        return operator.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ComputedColumnsOperator)
        {
            ComputedColumnsOperator that = (ComputedColumnsOperator) obj;
            return operator.equals(that.operator)
                && computedExpressions.equals(that.computedExpressions)
                && columns.equals(that.columns);
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        return String.format("COMPUTED COLUMNS (ID: %d, COLUMNS: %s)", nodeId, computedExpressions) + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }

    /** Result tuple */
    static class ComputedTuple implements Tuple.ComputedTuple
    {
        private final Tuple tuple;
        private final Object[] values;

        ComputedTuple(Tuple tuple, Object[] values)
        {
            this.tuple = tuple;
            this.values = values;
        }

        @Override
        public int getTupleOrdinal()
        {
            return tuple.getTupleOrdinal();
        }

        @Override
        public Tuple getTuple(int tupleOrdinal)
        {
            return tuple.getTuple(tupleOrdinal);
        }

        @Override
        public Object getValue(String column)
        {
            return tuple.getValue(column);
        }

        @Override
        public Object getValue(int columnOrdinal)
        {
            return tuple.getValue(columnOrdinal);
        }

        @Override
        public Iterator<TupleColumn> getColumns(int tupleOrdinal)
        {
            return tuple.getColumns(tupleOrdinal);
        }

        @Override
        public Object getComputedValue(int ordinal)
        {
            if (ordinal <= values.length)
            {
                return values[ordinal];
            }
            return null;
        }
    }
}
