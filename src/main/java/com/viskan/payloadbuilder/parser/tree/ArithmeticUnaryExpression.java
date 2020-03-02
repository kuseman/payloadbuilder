package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.codegen.CodeGenratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;

import static java.util.Objects.requireNonNull;

import org.apache.commons.lang3.NotImplementedException;

public class ArithmeticUnaryExpression extends Expression
{
    private final Type type;
    private final Expression expression;
    
    public ArithmeticUnaryExpression(Type type, Expression expression)
    {
        this.type = requireNonNull(type, "type");
        this.expression = requireNonNull(expression, "expression");
    }
    
    @Override
    public String toString()
    {
        return type.value + expression.toString();
    }
    
    public Type getType()
    {
        return type;
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
        ExpressionCode code = ExpressionCode.code(context);

        String method = null;
        switch (type)
        {
            case MINUS:
                method = "negate";
                break;
            case PLUS:
                throw new NotImplementedException("unary plus");
        }

        String template = 
              "%s"
            + "Object %s = null;\n"
            + "boolean %s = true;\n"
            + "if (!%s)\n"
            + "{\n"
            + "  %s = %s(%s);\n"
            + "  %s = false;\n"
            + "}\n";

        code.setCode(String.format(template,
                childCode.getCode(),
                code.getResVar(),
                code.getIsNull(),
                childCode.getIsNull(),
                code.getResVar(), method, childCode.getResVar(),
                code.getIsNull()));

        return code;
    }
    
    @Override
    public <TR, TC> TR accept(TreeVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
    
    public enum Type 
    {
        PLUS("+"),
        MINUS("-");
        private final String value;

        Type(String value) 
        {
            this.value = value;
        }

        public String getValue() 
        {
            return value;
        }
    }
}
