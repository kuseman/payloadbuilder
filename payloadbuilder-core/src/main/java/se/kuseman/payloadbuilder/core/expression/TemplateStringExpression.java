package se.kuseman.payloadbuilder.core.expression;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
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
    public ValueVector eval(final TupleVector input, IExecutionContext context)
    {
        int size = expressions.size();
        ValueVector[] vectors = new ValueVector[size];
        for (int i = 0; i < size; i++)
        {
            vectors[i] = expressions.get(i)
                    .eval(input, context);
        }

        if (size == 1)
        {
            return vectors[0];
        }

        int rowCount = input.getRowCount();

        MutableValueVector resultVector = context.getVectorFactory()
                .getMutableVector(ResolvedType.of(Type.String), rowCount);
        List<UTF8String> strings = new ArrayList<>(size);
        for (int i = 0; i < rowCount; i++)
        {
            resultVector.setString(i, evalInternal(strings, vectors, i));
        }
        return resultVector;
    }

    @Override
    public IExpression fold()
    {
        if (expressions.stream()
                .allMatch(IExpression::isConstant))
        {
            int size = expressions.size();
            ValueVector[] vectors = new ValueVector[size];
            for (int i = 0; i < size; i++)
            {
                vectors[i] = expressions.get(i)
                        .eval(null);
            }
            List<UTF8String> strings = new ArrayList<>(size);
            return new LiteralStringExpression(evalInternal(strings, vectors, 0));
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
        if (obj instanceof TemplateStringExpression that)
        {
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

    private UTF8String evalInternal(List<UTF8String> strings, ValueVector[] vectors, int row)
    {
        int size = vectors.length;
        strings.clear();
        for (int j = 0; j < size; j++)
        {
            if (vectors[j].isNull(row))
            {
                continue;
            }
            strings.add(UTF8String.from(vectors[j].valueAsObject(row)));
        }
        return UTF8String.concat(UTF8String.EMPTY, strings);
    }
}
