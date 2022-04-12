package se.kuseman.payloadbuilder.core.operator;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.operator.AOperator;
import se.kuseman.payloadbuilder.api.operator.DescribableNode;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Tuple;

/** Operator sorting target operator */
class OrderByOperator extends AOperator
{
    private final Operator target;
    private final TupleComparator comparator;

    OrderByOperator(int nodeId, Operator target, TupleComparator comparator)
    {
        super(nodeId);
        this.target = requireNonNull(target, "target");
        this.comparator = requireNonNull(comparator, "comparator");
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return singletonList(target);
    }

    @Override
    public String getName()
    {
        return "Order by";
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        return ofEntries(true, entry("Values", comparator.toString()));
    }

    @Override
    public TupleIterator open(IExecutionContext ctx)
    {
        ExecutionContext context = (ExecutionContext) ctx;
        List<Tuple> tuples = new ArrayList<>();
        TupleIterator it = target.open(context);
        if (it instanceof TupleList)
        {
            TupleList list = (TupleList) it;
            int size = list.size();
            for (int i = 0; i < size; i++)
            {
                tuples.add(list.get(i));
            }
        }
        else
        {
            while (it.hasNext())
            {
                tuples.add(it.next());
            }
            it.close();
        }
        Collections.sort(tuples, (a, b) -> comparator.compare(context, a, b));
        context.getStatementContext()
                .setTuple(null);
        return TupleIterator.wrap(tuples.iterator());
    }

    @Override
    public int hashCode()
    {
        return target.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof OrderByOperator)
        {
            OrderByOperator that = (OrderByOperator) obj;
            return target.equals(that.target)
                    && comparator.equals(that.comparator);
        }

        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        return String.format("ORDER BY (ID: %d, VALUES: %s)", nodeId, comparator) + System.lineSeparator() + indentString + target.toString(indent + 1);
    }
}
