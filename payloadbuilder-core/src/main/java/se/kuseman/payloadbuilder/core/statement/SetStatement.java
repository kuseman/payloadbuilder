package se.kuseman.payloadbuilder.core.statement;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Set statement */
public class SetStatement extends Statement
{
    private final String name;
    private final IExpression expression;
    private final boolean systemProperty;

    public SetStatement(String name, IExpression expression, boolean systemProperty)
    {
        this.name = requireNonNull(name, "name");
        this.expression = requireNonNull(expression, "expression");
        this.systemProperty = systemProperty;
    }

    public boolean isSystemProperty()
    {
        return systemProperty;
    }

    public String getName()
    {
        return name;
    }

    public IExpression getExpression()
    {
        return expression;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
