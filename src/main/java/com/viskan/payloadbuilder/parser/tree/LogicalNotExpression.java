package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.codegen.CodeGenratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;

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
    public ExpressionCode generateCode(CodeGenratorContext context, ExpressionCode parentCode)
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
    public <TR, TC> TR accept(TreeVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
    
    @Override
    public String toString()
    {
        return "NOT " + expression.toString();
    }
}
