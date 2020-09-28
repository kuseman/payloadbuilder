package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.parser.ExecutionContext;

/** Operator that wraps a sub operator and decorates it's
 * tuples with a subquery tuple */
class SubQueryOperator extends AOperator
{
    private final Operator operator;
    private final String alias;
    
    SubQueryOperator(Operator operator, String alias)
    {
        super(operator.getNodeId());
        this.operator = requireNonNull(operator, "operator");
        this.alias = requireNonNull(alias, "alias");
    }
    
    @Override
    public List<Operator> getChildOperators()
    {
        return operator.getChildOperators();
    }

    @Override
    public String getName()
    {
        return operator.getName();
    }

    @Override
    public String getDescribeString()
    {
        return operator.getDescribeString();
    }

    @Override
    public Map<String, Object> getDescribeProperties()
    {
        return operator.getDescribeProperties();
    }
    
    @Override
    public RowIterator open(ExecutionContext context)
    {
        final RowIterator it = operator.open(context);
        return new RowIterator()
        {
            @Override
            public Tuple next()
            {
                return new SubQueryTuple(it.next(), alias);
            }
            
            @Override
            public boolean hasNext()
            {
                return it.hasNext();
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
        if (obj instanceof SubQueryOperator)
        {
            SubQueryOperator that = (SubQueryOperator) obj;
            return operator.equals(that.operator)
                    && alias.equals(that.alias);
        }
        return false;
    }
    
    @Override
    public String toString()
    {
        return operator.toString();
    }
    
    @Override
    public String toString(int indent)
    {
        return operator.toString(indent);
    }
}
