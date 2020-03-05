package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.codegen.CodeGeneratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;
import com.viskan.payloadbuilder.evaluation.ExpressionMath;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.List;

public class InExpression extends Expression
{
    private final Expression expression;
    private final List<Expression> arguments;
    
    public InExpression(Expression expression, List<Expression> arguments)
    {
        this.expression = requireNonNull(expression, "expression");
        this.arguments = requireNonNull(arguments, "arguments");
    }
    
    public Expression getExpression()
    {
        return expression;
    }
    
    public List<Expression> getArguments()
    {
        return arguments;
    }
    
    @Override
    public Object eval(EvaluationContext evaluationContext, Row row)
    {
        Object value = expression.eval(evaluationContext, row);
        if (value == null)
        {
            return null;
        }
        
        for (Expression arg : arguments)
        {
            Object argValue = arg.eval(evaluationContext, row);
            if (argValue == null)
            {
                continue;
            }
            if (ExpressionMath.inValue(value, argValue))
            {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
    {
        ExpressionCode childCode = expression.generateCode(context, parentCode);
        ExpressionCode code = ExpressionCode.code(context);

        int size = arguments.size();
        // Generate code for
        StringBuilder sb = new StringBuilder();
        sb.append(childCode.getCode());

        sb.append("boolean ").append(code.getResVar()).append(" = false;\n");
        sb.append("boolean ").append(code.getIsNull()).append(" = ").append(childCode.getIsNull()).append(";\n");

        String template = "if (!%s && !%s)\n"
            + "{\n"
            + "  %s\n"
            + "  if (!%s)\n"
            + "  {\n"
            + "    %s = ExpressionMath.inValue(%s, %s);\n"
            + "    %s = false;\n"
            + "  }\n"
            + "}\n";

        for (int i = 0; i < size; i++)
        {
            Expression arg = arguments.get(i);
            ExpressionCode argCode = arg.generateCode(context, parentCode);
            sb.append(String.format(template,
                    code.getIsNull(), code.getResVar(),
                    argCode.getCode(),
                    argCode.getIsNull(),
                    code.getResVar(), childCode.getResVar(), argCode.getResVar(),
                    code.getIsNull()));
        }

        code.setCode(sb.toString());
        return code;
    }
    
    @Override
    public boolean isNullable()
    {
        return expression.isNullable() || arguments.stream().anyMatch(Expression::isNullable);
    }
    
    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
    
    @Override
    public String toString()
    {
        return expression.toString() + " IN (" + arguments.stream().map(e -> e.toString()).collect(joining(", ")) + ")";                
    }
}
