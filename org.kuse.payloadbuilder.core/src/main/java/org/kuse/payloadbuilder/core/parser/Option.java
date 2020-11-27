package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

/** Option (WITH) */
public class Option
{
    private final QualifiedName option;
    private final Expression valueExpression;

    public Option(QualifiedName option, Expression valueExpression)
    {
        this.option = requireNonNull(option, "option");
        this.valueExpression = requireNonNull(valueExpression, "valueExpression");
    }

    public QualifiedName getOption()
    {
        return option;
    }

    public Expression getValueExpression()
    {
        return valueExpression;
    }

    @Override
    public int hashCode()
    {
        return option.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof Option)
        {
            Option that = (Option) obj;
            return option.equals(that.option)
                && valueExpression.equals(that.valueExpression);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return option + "=" + valueExpression;
    }
}
