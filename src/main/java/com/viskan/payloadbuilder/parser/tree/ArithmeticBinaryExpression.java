package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.codegen.CodeGenratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;

import static java.util.Objects.requireNonNull;

public class ArithmeticBinaryExpression extends Expression
{
    private final Type type;
    private final Expression left;
    private final Expression right;
    
    public ArithmeticBinaryExpression(Type type, Expression left, Expression right)
    {
        this.type = requireNonNull(type, "type");
        this.left = requireNonNull(left, "left");
        this.right = requireNonNull(right, "right");
    }
    
    @Override
    public String toString()
    {
        return left.toString() + " " + type.value + " " + right.toString();
    }
    
    public Type getType()
    {
        return type;
    }
    public Expression getLeft()
    {
        return left;
    }
    
    public Expression getRight()
    {
        return right;
    }

    @Override
    public boolean isNullable()
    {
        return left.isNullable() || right.isNullable();
    }
    
    @Override
    public Class<?> getDataType()
    {
        return left.getDataType();
    }
    
    @Override
    public ExpressionCode generateCode(CodeGenratorContext context, ExpressionCode parentCode)
    {
        ExpressionCode leftCode = left.generateCode(context, parentCode);
        ExpressionCode rightCode = right.generateCode(context, parentCode);
        ExpressionCode code = ExpressionCode.code(context);

        String method = null;
        switch (type)
        {
            case ADD:
                method = "add";
                break;
            case SUBTRACT:
                method = "subtract";
                break;
            case MULTIPLY:
                method = "multiply";
                break;
            case DIVIDE:
                method = "divide";
                break;
            case MODULUS:
                method = "modulo";
                break;
        }

        String template = "%s"
            + "%s"
            + "Object %s = null;\n"
            + "boolean %s = true;\n"
            + "if (!%s && !%s)\n"
            + "{\n"
            + "  %s = %s(%s, %s);\n"
            + "  %s = false;\n"
            + "}\n";

        code.setCode(String.format(template,
                leftCode.getCode(),
                rightCode.getCode(),
                code.getResVar(),
                code.getIsNull(),
                leftCode.getIsNull(), rightCode.getIsNull(),
                code.getResVar(), method, leftCode.getResVar(), rightCode.getResVar(),
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
        ADD("+"),
        SUBTRACT("-"),
        MULTIPLY("*"),
        DIVIDE("/"),
        MODULUS("%");
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
