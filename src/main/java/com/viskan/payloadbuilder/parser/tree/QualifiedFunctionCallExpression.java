package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.catalog.FunctionInfo;
import com.viskan.payloadbuilder.codegen.CodeGenratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.List;

public class QualifiedFunctionCallExpression extends Expression
{
    private final List<Expression> arguments;
    private final FunctionInfo functionInfo;
    
    public QualifiedFunctionCallExpression(List<Expression> arguments, FunctionInfo functionInfo)
    {
        this.functionInfo = requireNonNull(functionInfo, "functionInfo");
        this.arguments = requireNonNull(arguments, "arguments");
    }
    
    @Override
    public ExpressionCode generateCode(CodeGenratorContext context, ExpressionCode parentCode)
    {
        return functionInfo.generateCode(context, parentCode, arguments);
    }
    
    @Override
    public <TR, TC> TR accept(TreeVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
    
    @Override
    public Class<?> getDataType()
    {
        return functionInfo.getDataType();
    }
    
    @Override
    public boolean isNullable()
    {
        return functionInfo.isNullable();
    }
    
    @Override
    public String toString()
    {
        return functionInfo.getName() + "(" + arguments.stream().map(a -> a.toString()).collect(joining(", ")) + ")";
    }
}
