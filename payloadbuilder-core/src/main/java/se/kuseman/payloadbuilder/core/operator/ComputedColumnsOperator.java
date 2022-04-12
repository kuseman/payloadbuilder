package se.kuseman.payloadbuilder.core.operator;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.TableMeta;
import se.kuseman.payloadbuilder.api.operator.AOperator;
import se.kuseman.payloadbuilder.api.operator.DescribableNode;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.IOrdinalValues;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.api.utils.MapUtils;

/**
 * Operator that computes columns from expressions.
 */
class ComputedColumnsOperator extends AOperator
{
    private final Operator operator;
    private final IOrdinalValuesFactory ordinalValuesFactory;
    private final int tupleOrdinal;

    ComputedColumnsOperator(int nodeId, int tupleOrdinal, Operator operator, IOrdinalValuesFactory ordinalValuesFactory)
    {
        super(nodeId);
        this.tupleOrdinal = tupleOrdinal;
        this.operator = requireNonNull(operator, "operator");
        this.ordinalValuesFactory = requireNonNull(ordinalValuesFactory, "ordinalValuesFactory");
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
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        return MapUtils.ofEntries(MapUtils.entry("Columns", ordinalValuesFactory));
    }

    @Override
    public TupleIterator open(IExecutionContext context)
    {
        return new ATupleIterator(operator.open(context))
        {
            @Override
            public Tuple process(Tuple tuple)
            {
                IOrdinalValues values = ordinalValuesFactory.create((ExecutionContext) context, tuple);
                return new ComputedTuple(tuple, values);
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
                    && ordinalValuesFactory.equals(that.ordinalValuesFactory);
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        return String.format("COMPUTED COLUMNS (ID: %d, EXPRESSIONS: %s)", nodeId, ordinalValuesFactory) + System.lineSeparator() + indentString + operator.toString(indent + 1);
    }

    /** Result tuple */
    class ComputedTuple implements Tuple
    {
        private final Tuple tuple;
        private final IOrdinalValues values;

        ComputedTuple(Tuple tuple, IOrdinalValues values)
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
        public int getColumnCount()
        {
            // Computed columns always shows the target tuples columns
            // not it's computed
            return tuple.getColumnCount();
        }

        @Override
        public int getColumnOrdinal(String column)
        {
            return tuple.getColumnOrdinal(column);
        }

        @Override
        public String getColumn(int ordinal)
        {
            return tuple.getColumn(ordinal);
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
        public Object getValue(int ordinal)
        {
            if (ordinal <= -1)
            {
                return null;
            }
            if (ordinal >= TableMeta.MAX_COLUMNS)
            {
                return values.getValue(ordinal - TableMeta.MAX_COLUMNS);
            }

            return tuple.getValue(ordinal);
        }
    }
}
