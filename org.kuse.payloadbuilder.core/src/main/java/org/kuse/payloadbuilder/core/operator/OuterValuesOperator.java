package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.iterators.SingletonIterator;
import org.apache.commons.lang3.StringUtils;
import org.kuse.payloadbuilder.core.operator.IIndexValuesFactory.IIndexValues;

/**
 * Operator used for index access in child operator. Ie.used in FROM operator with index access in where condition
 */
class OuterValuesOperator extends AOperator
{
    private final Operator operator;
    private final IIndexValuesFactory indexValuesFactory;

    OuterValuesOperator(int nodeId, Operator operator, IIndexValuesFactory indexValuesFactory)
    {
        super(nodeId);
        this.operator = requireNonNull(operator, "operator");
        this.indexValuesFactory = requireNonNull(indexValuesFactory, "indexValuesFactory");
    }

    @Override
    public String getName()
    {
        return "Outer values";
    }

    @Override
    public Map<String, Object> getDescribeProperties(ExecutionContext context)
    {
        return ofEntries(entry("Values", indexValuesFactory.toString()));
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        return singletonList(operator);
    }

    @Override
    public TupleIterator open(ExecutionContext context)
    {
        IIndexValues indexValues = indexValuesFactory.create(context, context.getStatementContext().getTuple());
        context.getStatementContext().setOuterIndexValues(new SingletonIterator<>(indexValues));
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
                && indexValuesFactory.equals(that.indexValuesFactory);
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        String desc = String.format("OUTER VALUES (ID: %d, OUTER VALUES: %s", nodeId, indexValuesFactory);
        return desc + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }
}
