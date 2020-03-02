package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.codegen.CodeGenratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;

import static java.util.Objects.requireNonNull;

public class ComparisonExpression extends Expression
{
    private final Type type;
    private final Expression left;
    private final Expression right;
    
    public ComparisonExpression(Type type, Expression left, Expression right)
    {
        this.type = requireNonNull(type, "type");
        this.left = requireNonNull(left, "left");
        this.right = requireNonNull(right, "right");
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
    public ExpressionCode generateCode(CodeGenratorContext context, ExpressionCode parentCode)
    {
        ExpressionCode leftCode = left.generateCode(context, parentCode);
        ExpressionCode rightCode = right.generateCode(context, parentCode);

        ExpressionCode code = ExpressionCode.code(context);
        StringBuilder sb = new StringBuilder();
        sb.append("// " + toString() + "\n");
        sb.append(leftCode.getCode());
        sb.append(rightCode.getCode());
        
        // TODO: nullable for faster comparisons
        //       Datatype checks, boolean
        String cmpOp = null;
        switch (type)
        {
            case EQUAL:
                cmpOp = "EQUAL";
                break;
            case NOT_EQUAL:
                cmpOp = "NOT_EQUAL";
                break;
            case GREATER_THAN:
                cmpOp = "GREATER_THAN";
                break;
            case GREATER_THAN_EQUAL:
                cmpOp = "GREATER_THAN_EQUAL";
                break;
            case LESS_THAN:
                cmpOp = "LESS_THAN";
                break;
            case LESS_THAN_EQUAL:
                cmpOp = "LESS_THAN_EQUAL";
                break;
        }

        String resultType = isNullable() ? "Boolean" : "boolean";
        code.setCode(String.format(
                  "%s"
                + "boolean %s = true;\n"
                + "%s %s = %s;\n"
                + "if (!%s && !%s)\n"
                + "{\n"
                + "  %s = compare(%s, %s, %s);\n"
                + "  %s = false;\n"
                + "}\n",
                sb.toString(),
                code.getIsNull(),
                resultType, code.getResVar(), isNullable() ? "null" : "false",
                leftCode.getIsNull(), rightCode.getIsNull(),
                code.getResVar(), leftCode.getResVar(), rightCode.getResVar(), cmpOp,
                code.getIsNull()));

        return code;
    }
    
    @Override
    public Class<?> getDataType()
    {
        return Boolean.class;
    }
    
    @Override
    public boolean isNullable()
    {
        return left.isNullable() || right.isNullable();
    }
    
    @Override
    public String toString()
    {
        return left.toString() + " " + type.value + " " + right.toString();
    }
    
    @Override
    public <TR, TC> TR accept(TreeVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
    
    public enum Type 
    {
        EQUAL("="),
        NOT_EQUAL("!="),
        LESS_THAN("<"),
        LESS_THAN_EQUAL("<="),
        GREATER_THAN(">"),
        GREATER_THAN_EQUAL(">=");
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
