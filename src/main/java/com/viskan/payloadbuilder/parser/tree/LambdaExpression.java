package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.codegen.CodeGeneratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.Arrays;
import java.util.List;

public class LambdaExpression extends Expression
{
    private final List<String> identifiers;
    private final Expression expression;
    /**
     * <pre>
     *  Unique ids for identifying lambda identifiers.
     * Used to retrieve the underlying value contained in the identifier
     * during evaluation.
     * </pre>
     */
    private final int[] lambdaIds;

    public LambdaExpression(List<String> identifiers, Expression expression, int[] lambdaIds)
    {
        this.identifiers = requireNonNull(identifiers, "identifiers");
        this.expression = requireNonNull(expression);
        this.lambdaIds = lambdaIds;
    }

    public List<String> getIdentifiers()
    {
        return identifiers;
    }

    public Expression getExpression()
    {
        return expression;
    }

    public int[] getLambdaIds()
    {
        return lambdaIds;
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public boolean isNullable()
    {
        throw new IllegalArgumentException("Labmda expressions");
    }

    @Override
    public Class<?> getDataType()
    {
        throw new IllegalArgumentException("Labmda expressions");
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
    {
        throw new IllegalArgumentException("Labmda expressions");
    }

    @Override
    public int hashCode()
    {
        return 17 +
            37 * identifiers.hashCode() +
            37 * expression.hashCode() +
            37 * Arrays.hashCode(lambdaIds);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof LambdaExpression)
        {
            LambdaExpression le = (LambdaExpression) obj;
            return identifiers.equals(le.identifiers)
                &&
                expression.equals(le.expression)
                &&
                Arrays.equals(lambdaIds, le.lambdaIds);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return (identifiers.size() > 1 ? "(" : "") +
            identifiers.stream().map(i -> i.toString()).collect(joining(",")) +
            (identifiers.size() > 1 ? ")" : "") +
            " -> " + expression.toString();
    }
}
