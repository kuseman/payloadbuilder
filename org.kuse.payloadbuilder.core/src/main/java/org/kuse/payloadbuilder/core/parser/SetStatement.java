package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.join;

/** Set statement */
public class SetStatement extends Statement
{
    private final String name;
    private final Expression expression;
    private final boolean systemProperty;

    SetStatement(QualifiedName qname, Expression expression, boolean systemProperty)
    {
        this.name = join(qname.getParts(), ".");
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

    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
