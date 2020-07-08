package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.Expression;

import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.iterators.SingletonIterator;
import org.apache.commons.lang3.StringUtils;

/**
 * Operator used for index access in child operator. Ie.used in FROM operator with index access in where condition
 */
class OuterValuesOperator extends AOperator
{
    private final Operator operator;
    private final List<Expression> valueExpressions;

    OuterValuesOperator(int nodeId, Operator operator, List<Expression> valueExpressions)
    {
        super(nodeId);
        this.operator = requireNonNull(operator, "operator");
        this.valueExpressions = requireNonNull(valueExpressions, "valueExpressions");
    }
    
    @Override
    public String getName()
    {
        return "Outer values";
    }
    
    @Override
    public Map<String, Object> getDescribeProperties()
    {
        return ofEntries(entry("Values", valueExpressions.toString()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Row> open(ExecutionContext context)
    {
        int size = valueExpressions.size();
        Object[] outerValues = new Object[size];

        for (int i = 0; i < size; i++)
        {
            outerValues[i] = valueExpressions.get(i).eval(context);
        }

        context.getOperatorContext().setOuterIndexValues(new SingletonIterator(outerValues));
        return operator.open(context);
    }

    @Override
    public int hashCode()
    {
        return 17 +
            (37 * operator.hashCode());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof OuterValuesOperator)
        {
            OuterValuesOperator that = (OuterValuesOperator) obj;
            return operator.equals(that.operator)
                && valueExpressions.equals(that.valueExpressions);
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        String desc = String.format("OUTER VALUES (ID: %d, OUTER VALUES: %s", nodeId, valueExpressions);
        return desc + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }

}
