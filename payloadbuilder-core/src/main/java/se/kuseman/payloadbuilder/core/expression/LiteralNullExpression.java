package se.kuseman.payloadbuilder.core.expression;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.ILiteralNullExpression;

/** Null expression */
public class LiteralNullExpression extends LiteralExpression implements ILiteralNullExpression
{
    public LiteralNullExpression(ResolvedType type)
    {
        super(type);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        return ValueVector.literalNull(type, input.getRowCount());
    }

    @Override
    public ValueVector eval(TupleVector input, ValueVector selection, IExecutionContext context)
    {
        return ValueVector.literalNull(type, selection.size());
    }

    @Override
    public int hashCode()
    {
        return 0;
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof LiteralNullExpression;
    }

    @Override
    public String toString()
    {
        return "null";
    }
}
