package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.codegen.CodeGeneratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

import static com.viskan.payloadbuilder.evaluation.ExpressionMath.eq;
import static com.viskan.payloadbuilder.evaluation.ExpressionMath.gt;
import static com.viskan.payloadbuilder.evaluation.ExpressionMath.gte;
import static com.viskan.payloadbuilder.evaluation.ExpressionMath.lt;
import static com.viskan.payloadbuilder.evaluation.ExpressionMath.lte;
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
    public Object eval(EvaluationContext evaluationContext, Row row)
    {
        Object leftResult = left.eval(evaluationContext, row);
        Object rightResult = right.eval(evaluationContext, row);

        if (leftResult == null || rightResult == null)
        {
            return null;
        }

        switch (type)
        {
            case EQUAL:
                return eq(leftResult, rightResult);
            case NOT_EQUAL:
                return !eq(leftResult, rightResult);
            case GREATER_THAN:
                return gt(leftResult, rightResult);
            case GREATER_THAN_EQUAL:
                return gte(leftResult, rightResult);
            case LESS_THAN:
                return lt(leftResult, rightResult);
            case LESS_THAN_EQUAL:
                return lte(leftResult, rightResult);
            default:
                throw new IllegalArgumentException("Unkown comparison operator: " + type);
        }
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
    {
        ExpressionCode leftCode = left.generateCode(context, parentCode);
        ExpressionCode rightCode = right.generateCode(context, parentCode);

        ExpressionCode code = ExpressionCode.code(context);

        String cmpOp = null;
        switch (type)
        {
            case EQUAL:
                cmpOp = "ExpressionMath.eq";
                break;
            case NOT_EQUAL:
                cmpOp = "!ExpressionMath.eq";
                break;
            case GREATER_THAN:
                cmpOp = "ExpressionMath.gt";
                break;
            case GREATER_THAN_EQUAL:
                cmpOp = "ExpressionMath.gte";
                break;
            case LESS_THAN:
                cmpOp = "ExpressionMath.lt";
                break;
            case LESS_THAN_EQUAL:
                cmpOp = "ExpressionMath.lte";
                break;
        }

        code.setCode(String.format(
                      "%s"
                    + "boolean %s = true;\n"
                    + "boolean %s = false;\n"
                    + "if (!%s)\n"
                    + "{\n"
                    + "  %s"
                    + "  if (!%s)\n"
                    + "  {\n"
                    + "    %s = %s(%s, %s);\n"
                    + "    %s = false;\n"
                    + "  }\n"
                    + "}\n",
                leftCode.getCode(),
                code.getIsNull(),
                code.getResVar(), 
                leftCode.getIsNull(), 
                rightCode.getCode(),
                rightCode.getIsNull(),
                code.getResVar(), cmpOp, leftCode.getResVar(), rightCode.getResVar(),
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
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
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
