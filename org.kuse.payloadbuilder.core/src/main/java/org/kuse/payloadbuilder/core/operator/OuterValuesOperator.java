package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.iterators.SingletonIterator;
import org.apache.commons.lang3.StringUtils;
import org.kuse.payloadbuilder.core.operator.IOrdinalValuesFactory.IOrdinalValues;

/**
 * Operator used for index access in child operator. Ie.used in FROM operator with index access in where condition
 */
class OuterValuesOperator extends AOperator
{
    private final Operator operator;
    private final IOrdinalValuesFactory ordinalValuesFactory;

    OuterValuesOperator(int nodeId, Operator operator, IOrdinalValuesFactory ordinalValuesFactory)
    {
        super(nodeId);
        this.operator = requireNonNull(operator, "operator");
        this.ordinalValuesFactory = requireNonNull(ordinalValuesFactory, "ordinalValuesFactory");
    }

    @Override
    public String getName()
    {
        return "Outer values";
    }

    @Override
    public Map<String, Object> getDescribeProperties(ExecutionContext context)
    {
        return ofEntries(entry("Values", ordinalValuesFactory.toString()));
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return singletonList(operator);
    }

    @Override
    public TupleIterator open(ExecutionContext context)
    {
        IOrdinalValues ordinalValues = ordinalValuesFactory.create(context, context.getStatementContext().getTuple());
        context.getStatementContext().setOuterOrdinalValues(new SingletonIterator<>(ordinalValues));
        return operator.open(context);
    }

    @Override
    public int hashCode()
    {
        return operator.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof OuterValuesOperator)
        {
            OuterValuesOperator that = (OuterValuesOperator) obj;
            return operator.equals(that.operator)
                && ordinalValuesFactory.equals(that.ordinalValuesFactory);
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        String desc = String.format("OUTER VALUES (ID: %d, OUTER VALUES: %s", nodeId, ordinalValuesFactory);
        return desc + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }
}
