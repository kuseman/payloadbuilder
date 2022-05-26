package se.kuseman.payloadbuilder.core.operator;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.operator.AOperator;
import se.kuseman.payloadbuilder.api.operator.DescribableNode;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Tuple;

/** Filtering operator */
class FilterOperator extends AOperator
{
    private final Operator operator;
    private final Predicate<ExecutionContext> predicate;

    FilterOperator(int nodeId, Operator operator, Predicate<ExecutionContext> predicate)
    {
        super(nodeId);
        this.operator = requireNonNull(operator);
        this.predicate = requireNonNull(predicate);
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return singletonList(operator);
    }

    @Override
    public String getName()
    {
        return "Filter";
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        return ofEntries(true, entry(DescribableNode.PREDICATE, predicate));
    }

    @Override
    public TupleIterator open(IExecutionContext context)
    {
        return new ATupleIterator(operator.open(context))
        {
            @Override
            protected boolean setNext(Tuple tuple)
            {
                StatementContext sctx = (StatementContext) context.getStatementContext();
                sctx.setTuple(tuple);
                boolean result = predicate.test((ExecutionContext) context);
                sctx.setTuple(null);
                return result;
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
        if (obj instanceof FilterOperator)
        {
            FilterOperator that = (FilterOperator) obj;
            return nodeId.equals(that.nodeId)
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
        return desc + System.lineSeparator() + indentString + operator.toString(indent + 1);
    }
}
