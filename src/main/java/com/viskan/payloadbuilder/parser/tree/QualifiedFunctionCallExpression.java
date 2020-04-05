package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;
import com.viskan.payloadbuilder.codegen.CodeGeneratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.List;

public class QualifiedFunctionCallExpression extends Expression
{
    private final ScalarFunctionInfo functionInfo;
    private final List<Expression> arguments;

    public QualifiedFunctionCallExpression(ScalarFunctionInfo functionInfo, List<Expression> arguments)
    {
        this.functionInfo = requireNonNull(functionInfo, "functionInfo");
        this.arguments = requireNonNull(arguments, "arguments");
    }

    public ScalarFunctionInfo getFunctionInfo()
    {
        return functionInfo;
    }

    public List<Expression> getArguments()
    {
        return arguments;
    }

    @Override
    public Object eval(EvaluationContext evaluationContext, Row row)
    {
        return functionInfo.eval(evaluationContext, arguments, row);
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
    {
        return functionInfo.generateCode(context, parentCode, arguments);
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
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
    public int hashCode()
    {
        return 17 +
            (37 * functionInfo.hashCode()) +
            (37 * arguments.hashCode());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof QualifiedFunctionCallExpression)
        {
            QualifiedFunctionCallExpression qfce = (QualifiedFunctionCallExpression) obj;
            return functionInfo.equals(qfce.functionInfo)
                &&
                arguments.equals(qfce.arguments);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return functionInfo.getName() + "(" + arguments.stream().map(a -> a.toString()).collect(joining(", ")) + ")";
    }
}
