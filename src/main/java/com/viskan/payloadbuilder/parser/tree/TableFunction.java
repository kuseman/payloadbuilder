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
        super(alias);
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
}
