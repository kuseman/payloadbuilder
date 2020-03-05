package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.codegen.CodeGeneratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

import static java.util.Objects.requireNonNull;

public class LogicalNotExpression extends Expression
{
    private final Expression expression;
    
    public LogicalNotExpression(Expression expression)
    {
        this.expression = requireNonNull(expression, "expression");
    }
    
    public Expression getExpression()
    {
        return expression;
    }
    
    @Override
    public boolean isNullable()
    {
        return expression.isNullable();
    }
    
    @Override
    public Class<?> getDataType()
    {
        return expression.getDataType();
    }
    
    @Override
    public Object eval(EvaluationContext evaluationContext, Row row)
    {
        Object result = expression.eval(evaluationContext, row);
        return result != null ? !(Boolean) result : null;
    }
    
    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
    {
        ExpressionCode childCode = expression.generateCode(context, parentCode);

        if (expression.isNullable())
        {
            boolean addCast = !Boolean.class.isAssignableFrom(expression.getDataType());

            String template = "%s\n"
                + "if (!%s)\n"
                + "{\n"
                + "  %s = !%s%s;\n"
                + "}\n";

            childCode.setCode(String.format(template,
                    childCode.getCode(),
                    childCode.getIsNull(),
                    childCode.getResVar(),
                    addCast ? "(Boolean)" : "", childCode.getResVar()));
        }
        else
        {
            String template = "%s\n"
                + "%s = !%s;\n";
            childCode.setCode(String.format(template,
                    childCode.getCode(),
                    childCode.getResVar(), childCode.getResVar()));
        }

        childCode.setCode("// NOT\n" + childCode.getCode());
        return childCode;    
    }
    
    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
    
    @Override
    public String toString()
    {
        return "NOT " + expression.toString();
    }
}
