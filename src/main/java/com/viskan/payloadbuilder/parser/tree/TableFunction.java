package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.catalog.TableFunctionInfo;

import static java.util.Objects.requireNonNull;

import java.util.List;

public class TableFunction extends TableSource
{
    private final TableFunctionInfo functionInfo;
    private final List<Expression> arguments;

    public TableFunction(TableFunctionInfo functionInfo, List<Expression> arguments, String alias)
    {
        super(alias, false);
        this.functionInfo = requireNonNull(functionInfo, "functionInfo");
        this.arguments = requireNonNull(arguments, "arguments");
    }
    
    public TableFunctionInfo getFunctionInfo()
    {
        return functionInfo;
    }
    
    public List<Expression> getArguments()
    {
        return arguments;
    }
    
    @Override
    public <TR, TC> TR accept(TreeVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);                
    }
    
    @Override
    public String toString()
    {
        return functionInfo + " " + alias;
    }
}
