package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

import java.nio.charset.StandardCharsets;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.ILiteralStringExpression;

/** Literal string */
public class LiteralStringExpression extends LiteralExpression implements ILiteralStringExpression
{
    private final UTF8String string;

    public LiteralStringExpression(String string)
    {
        this(UTF8String.utf8(string.getBytes(StandardCharsets.UTF_8)));
    }

    public LiteralStringExpression(UTF8String string)
    {
        super(ResolvedType.of(Type.String));
        this.string = requireNonNull(string, "string");
    }

    @Override
    public UTF8String getValue()
    {
        return string;
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        return ValueVector.literalString(string, input.getRowCount());
    }

    @Override
    public ValueVector eval(TupleVector input, ValueVector selection, IExecutionContext context)
    {
        return ValueVector.literalString(string, selection.size());
    }

    @Override
    public int hashCode()
    {
        return string.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof LiteralStringExpression)
        {
            return string.equals(((LiteralStringExpression) obj).string);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "'" + string + "'";
    }
}
