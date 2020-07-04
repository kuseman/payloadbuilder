package com.viskan.payloadbuilder.parser;

import static java.util.Objects.requireNonNull;

/** Table source option (WITH) */
public class TableOption
{
    private final QualifiedName option;
    private final Expression valueExpression;
    
    TableOption(QualifiedName option, Expression valueExpression)
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
        return 37
                + (17 * option.hashCode())
                + (17 * valueExpression.hashCode());
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof TableOption)
        {
            TableOption that = (TableOption) obj;
            return option.equals(that.option)
                    && valueExpression.equals(that.valueExpression);
        }
        return false;
    }
    
}
