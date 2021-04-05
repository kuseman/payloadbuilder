package org.kuse.payloadbuilder.core.parser;

import static org.apache.commons.lang3.StringUtils.join;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import org.kuse.payloadbuilder.core.operator.ExecutionContext;

/** A variable (@var) */
public class VariableExpression extends Expression implements HasIdentifier
{
    private final String name;
    private final boolean system;

    public VariableExpression(QualifiedName qname)
    {
        this(qname, false);
    }

    public VariableExpression(QualifiedName qname, boolean system)
    {
        this.name = lowerCase(join(qname.getParts(), "."));
        this.system = system;
    }

    public String getName()
    {
        return name;
    }

    @Override
    public String identifier()
    {
        return name;
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public Object eval(ExecutionContext context)
    {
        if (system)
        {
            if ("rowcount".equals(name))
            {
                return context.getRowCount();
            }
            else if ("version".equals(name))
            {
                return context.getVersionString();
            }

            return null;
        }

        return context.getVariableValue(name);
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof VariableExpression)
        {
            VariableExpression that = (VariableExpression) obj;
            return name.equals(that.name)
                && system == that.system;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "@" + (system ? "@" : "") + name;
    }
}
