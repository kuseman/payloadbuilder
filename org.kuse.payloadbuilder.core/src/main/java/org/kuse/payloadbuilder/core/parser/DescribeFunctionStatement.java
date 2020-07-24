package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

/** DESCRIBE function() */
public class DescribeFunctionStatement extends Statement
{
    private final String catalog;
    private final String functionName;

    DescribeFunctionStatement(String catalog, String functionName)
    {
        this.catalog = catalog;
        this.functionName = requireNonNull(functionName, "functionName");
    }
    
    public String getCatalog()
    {
        return catalog;
    }
    
    public String getFunctionName()
    {
        return functionName;
    }

    @Override
    public <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
}
