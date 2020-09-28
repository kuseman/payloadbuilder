package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

import org.apache.commons.lang3.StringUtils;
import org.kuse.payloadbuilder.core.DescribeUtils;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;

/** Filtering operator */
class FilterOperator extends AOperator
{
    private final Operator operator;
    private final BiPredicate<ExecutionContext, Tuple> predicate;

    FilterOperator(int nodeId, Operator operator, BiPredicate<ExecutionContext, Tuple> predicate)
    {
        super(nodeId);
        this.operator = requireNonNull(operator);
        this.predicate = requireNonNull(predicate);
    }

    @Override
    public List<Operator> getChildOperators()
    {
        return asList(operator);
    }

    @Override
    public String getName()
    {
        return "Filter";
    }

    @Override
    public Map<String, Object> getDescribeProperties()
    {
        return ofEntries(true,
                entry(DescribeUtils.PREDICATE, predicate));
    }

    @Override
    public RowIterator open(ExecutionContext context)
    {
        RowIterator iterator = operator.open(context);
        return new RowIterator()
        {
            private Tuple next;

            @Override
            public Tuple next()
            {
                Tuple result = next;
                next = null;
                return result;
            }

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            @Override
            public void close()
            {
                iterator.close();
            }

            private boolean setNext()
            {
                while (next == null)
                {
                    if (!iterator.hasNext())
                    {
                        return false;
                    }

                    Tuple tuple = iterator.next();
                    if (predicate.test(context, tuple))
                    {
                        next = tuple;
                    }
                }
                return true;
            }
        };
    }

    @Override
    public int hashCode()
    {
        return 17 +
            37 * operator.hashCode() +
            37 * predicate.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof FilterOperator)
        {
            FilterOperator that = (FilterOperator) obj;
            return nodeId == that.nodeId
                && operator.equals(that.operator)
                && predicate.equals(that.predicate);
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        String desc = String.format("FILTER (ID: %d, EXECUTION COUNT: %s, PREDICATE: %s)", nodeId, 0, predicate);
        return desc + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }
}
