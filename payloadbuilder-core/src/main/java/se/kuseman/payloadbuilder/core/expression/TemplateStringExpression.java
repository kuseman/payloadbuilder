package se.kuseman.payloadbuilder.core.expression;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.UTF8String;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.ITemplateStringExpression;

/** A back tick or template string expression. */
public class TemplateStringExpression implements ITemplateStringExpression
{
    private final List<IExpression> expressions;

    public TemplateStringExpression(List<IExpression> expressions)
    {
        this.expressions = unmodifiableList(requireNonNull(expressions, "expressions"));
    }

    @Override
    public List<IExpression> getExpressions()
    {
        return expressions;
    }

    @Override
    public List<IExpression> getChildren()
    {
        return expressions;
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public ValueVector eval(final TupleVector input, IExecutionContext context)
    {
        final int size = expressions.size();
        final ValueVector[] vectors = new ValueVector[size];
        for (int i = 0; i < size; i++)
        {
            vectors[i] = expressions.get(i)
                    .eval(input, context);
        }
        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.String);
            }

            @Override
            public int size()
            {
                return input.getRowCount();
            }

            @Override
            public boolean isNull(int row)
            {
                // Can this be null ?
                return false;
            }

            @Override
            public UTF8String getString(int row)
            {
                List<UTF8String> strings = new ArrayList<>(size);
                for (int i = 0; i < size; i++)
                {
                    strings.add(vectors[i].getString(row));
                }
                return UTF8String.concat(UTF8String.EMPTY, strings);
            }

            @Override
            public Object getValue(int row)
            {
                throw new IllegalArgumentException("getValue should not be called");
            }
        };
    }

    @Override
    public IExpression fold()
    {
        if (expressions.stream()
                .allMatch(IExpression::isConstant))
        {
            ValueVector valueVector = eval(TupleVector.CONSTANT, null);
            return new LiteralStringExpression(valueVector.getString(0));
        }

        return this;
    }

    @Override
    public ResolvedType getType()
    {
        return ResolvedType.of(Type.String);
    }

    @Override
    public int hashCode()
    {
        return expressions.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof TemplateStringExpression)
        {
            TemplateStringExpression that = (TemplateStringExpression) obj;
            return expressions.equals(that.expressions);
        }
        return false;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("`");
        for (IExpression e : expressions)
        {
            sb.append(e.toString());
        }
        sb.append("`");
        return sb.toString();
    }

    @Override
    public String toVerboseString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("`");
        for (IExpression e : expressions)
        {
            sb.append(e.toVerboseString());
        }
        sb.append("`");
        return sb.toString();
    }
}
