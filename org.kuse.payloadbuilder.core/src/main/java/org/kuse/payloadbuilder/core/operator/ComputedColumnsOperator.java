package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.utils.MapUtils;

/**
 * Operator that computes columns from expressions.
 */
class ComputedColumnsOperator extends AOperator
{
    private final Operator operator;
    private final List<Expression> computedExpressions;
    private final String[] columns;
    private final int tupleOrdinal;

    ComputedColumnsOperator(int nodeId, int tupleOrdinal, Operator operator, List<String> columns, List<Expression> computedExpressions)
    {
        super(nodeId);
        this.tupleOrdinal = tupleOrdinal;
        this.operator = requireNonNull(operator, "operator");
        this.columns = requireNonNull(columns, "columns").toArray(ArrayUtils.EMPTY_STRING_ARRAY);
        this.computedExpressions = requireNonNull(computedExpressions, "computedExpressions");
    }

    @Override
    public List<DescribableNode> getChildNodes()
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
        int size = columns.length;
        return MapUtils.ofEntries(MapUtils.entry("Columns", IntStream.range(0, size).mapToObj(i -> String.format("%s: %s", columns[i], computedExpressions.get(i))).collect(joining())));
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
                    values[i] = EvalUtils.unwrap(context, computedExpressions.get(i).eval(context));
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
                && Arrays.equals(columns, that.columns);
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        return String.format("COMPUTED COLUMNS (ID: %d, COLUMNS: %s, EXPRESSIONS: %s)", nodeId, Arrays.toString(columns), computedExpressions) + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }

    /** Result tuple */
    class ComputedTuple implements Tuple
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
            return tupleOrdinal;
        }

        @Override
        public Tuple getTuple(int tupleOrdinal)
        {
            // Keep this context if we access the target computed tuple
            if (tupleOrdinal == ComputedColumnsOperator.this.tupleOrdinal)
            {
                return this;
            }
            return tuple.getTuple(tupleOrdinal);
        }

        @Override
        public int getColumnCount()
        {
            return tuple.getColumnCount() + values.length;
        }

        @Override
        public int getColumnOrdinal(String column)
        {
            int ordinal = ArrayUtils.indexOf(columns, column);
            if (ordinal >= 0)
            {
                return ordinal;
            }

            return tuple.getColumnOrdinal(column) + values.length;
        }

        @Override
        public String getColumn(int ordinal)
        {
            if (ordinal < columns.length)
            {
                return columns[ordinal];
            }
            return tuple.getColumn(ordinal - columns.length);
        }

        @Override
        public Object getValue(int columnOrdinal)
        {
            if (columnOrdinal == -1)
            {
                return null;
            }
            if (columnOrdinal < columns.length)
            {
                return values[columnOrdinal];
            }
            return tuple.getValue(columnOrdinal - columns.length);
        }
    }
}
