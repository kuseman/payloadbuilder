package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.catalog.FunctionInfo;

import static java.util.Objects.requireNonNull;

import java.util.List;

/** Definition of a function call */
public class FunctionCall
{
    private final FunctionInfo functionInfo;
    private final List<Expression> arguments;
    
    public FunctionCall(FunctionInfo functionInfo, List<Expression> arguments)
    {
        this.functionInfo = requireNonNull(functionInfo, "functionInfo");
        this.arguments = requireNonNull(arguments, "arguments");
    }
    
    public FunctionInfo getFunctionInfo()
    {
        return functionInfo;
    }
    
    public List<Expression> getArguments()
    {
        return arguments;
    }
}
