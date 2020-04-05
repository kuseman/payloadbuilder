package com.viskan.payloadbuilder.parser.tree;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.codegen.CodeGeneratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

import static com.viskan.payloadbuilder.evaluation.ExpressionMath.add;
import static com.viskan.payloadbuilder.evaluation.ExpressionMath.divide;
import static com.viskan.payloadbuilder.evaluation.ExpressionMath.modulo;
import static com.viskan.payloadbuilder.evaluation.ExpressionMath.multiply;
import static com.viskan.payloadbuilder.evaluation.ExpressionMath.subtract;
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
            case ADD:
                return add(leftResult, rightResult);
            case SUBTRACT:
                return subtract(leftResult, rightResult);
            case MULTIPLY:
                return multiply(leftResult, rightResult);
            case DIVIDE:
                return divide(leftResult, rightResult);
            case MODULUS:
                return modulo(leftResult, rightResult);
            default:
                throw new IllegalArgumentException("Unknown operator " + type);
        }
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
    {
        ExpressionCode leftCode = left.generateCode(context, parentCode);
        ExpressionCode rightCode = right.generateCode(context, parentCode);
        ExpressionCode code = ExpressionCode.code(context);

        String method = null;
        switch (type)
        {
            case ADD:
                method = "ExpressionMath.add";
                break;
            case SUBTRACT:
                method = "ExpressionMath.subtract";
                break;
            case MULTIPLY:
                method = "ExpressionMath.multiply";
                break;
            case DIVIDE:
                method = "ExpressionMath.divide";
                break;
            case MODULUS:
                method = "ExpressionMath.modulo";
                break;
        }

        String template = "%s"
            + "Object %s = null;\n"
            + "boolean %s = true;\n"
            + "if (!%s)\n"
            + "{\n"
            + "  %s"
            + "  if (!%s)\n"
            + "  {"
            + "    %s = %s(%s, %s);\n"
            + "    %s = false;\n"
            + "  }\n"
            + "}\n";

        code.setCode(String.format(template,
                leftCode.getCode(),
                code.getResVar(),
                code.getIsNull(),
                leftCode.getIsNull(),
                rightCode.getCode(),
                rightCode.getIsNull(),
                code.getResVar(), method, leftCode.getResVar(), rightCode.getResVar(),
                code.getIsNull()));

        return code;
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public String toString()
    {
        return left.toString() + " " + type.value + " " + right.toString();
    }

    @Override
    public int hashCode()
    {
        return 17 +
            (37 * left.hashCode()) +
            (37 * right.hashCode()) +
            (37 * type.hashCode());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ArithmeticBinaryExpression)
        {
            ArithmeticBinaryExpression e = (ArithmeticBinaryExpression) obj;
            return left.equals(e.getLeft())
                &&
                right.equals(e.getRight())
                &&
                type == e.type;
        }
        return false;
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
