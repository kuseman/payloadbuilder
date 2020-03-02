package com.viskan.payloadbuilder.parser.tree;

import static java.util.Objects.requireNonNull;

public class ExpressionSelectItem extends SelectItem
{
    private final Expression expression;

    public ExpressionSelectItem(Expression expression, String identifier)
    {
        super(identifier);
        this.expression = requireNonNull(expression, "expression");
    }

    @Override
    public String toString()
    {
        return expression.toString() + super.toString();
    }
}
