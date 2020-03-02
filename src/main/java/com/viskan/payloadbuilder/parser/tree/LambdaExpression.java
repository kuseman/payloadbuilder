package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.codegen.CodeGenratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.List;

public class LambdaExpression extends Expression
{
    private final List<String> identifiers;
    private final Expression expression;

    public LambdaExpression(List<String> identifiers, Expression expression)
    {
        this.identifiers = requireNonNull(identifiers, "identifiers");
        this.expression = requireNonNull(expression);
    }
    
    public List<String> getIdentifiers()
    {
        return identifiers;
    }

    public Expression getExpression()
    {
        return expression;
    }
    
    @Override
    public <TR, TC> TR accept(TreeVisitor<TR, TC> visitor, TC context)
    {
        throw new IllegalArgumentException("Labmda expressions");
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
    public ExpressionCode generateCode(CodeGenratorContext context, ExpressionCode parentCode)
    {
        throw new IllegalArgumentException("Labmda expressions");
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
