package se.kuseman.payloadbuilder.core.common;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Option (WITH) */
public class Option
{
    private final QualifiedName option;
    private final IExpression valueExpression;

    public Option(QualifiedName option, IExpression valueExpression)
    {
        this.option = requireNonNull(option, "option");
        this.valueExpression = requireNonNull(valueExpression, "valueExpression");
    }

    public QualifiedName getOption()
    {
        return option;
    }

    public IExpression getValueExpression()
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
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof Option)
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
