package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;
import static org.kuse.payloadbuilder.core.utils.ExpressionMath.eq;
import static org.kuse.payloadbuilder.core.utils.ExpressionMath.gt;
import static org.kuse.payloadbuilder.core.utils.ExpressionMath.gte;
import static org.kuse.payloadbuilder.core.utils.ExpressionMath.lt;
import static org.kuse.payloadbuilder.core.utils.ExpressionMath.lte;

import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;

/** Comparison expression */
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
    public boolean isConstant()
    {
        return left.isConstant() && right.isConstant();
    }

    @Override
    public Expression fold()
    {
        boolean ll = left instanceof LiteralExpression;
        boolean rl = right instanceof LiteralExpression;

        if (ll && rl)
        {
            return LiteralExpression.create(evalInternal(
                    ((LiteralExpression) left).getObjectValue(),
                    ((LiteralExpression) right).getObjectValue()));
        }

        return this;
    }

    @Override
    public Object eval(ExecutionContext context)
    {
        Object leftResult = left.eval(context);
        Object rightResult = right.eval(context);
        return evalInternal(leftResult, rightResult);
    }

    @Override
    public boolean isCodeGenSupported()
    {
        return left.isCodeGenSupported() && right.isCodeGenSupported();
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context)
    {
        ExpressionCode code = context.getCode();

        ExpressionCode leftCode = left.generateCode(context);
        ExpressionCode rightCode = right.generateCode(context);

        String cmpOp = null;
        //CSOFF
        switch (type)
        //CSON
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

        //        * Object v1 = tuple.getTuple(1).getValue("country_id");
        //        * Boolean v3 = false;
        //        * if (v1 != null)
        //        * {
        //        *   Object v2 = tuple.getTuple(0).getValue("country_id");
        //        *   v3 = v2 != null && ExpressionMath.eq(v1, v2);
        //        * }

        code.setCode(String.format(
                "%s"                                      // Leftcode
                    + "Boolean %s = false;\n"                 // This result var
                    + "if (%s != null)\n"                     // Left res var
                    + "{\n"
                    + "  %s"                                  // Right code
                    + "  %s = %s != null && %s(%s, %s);\n"    // ExpressionMath-function
                    + "}\n",
                leftCode.getCode(),
                code.getResVar(),
                leftCode.getResVar(),
                rightCode.getCode(),
                code.getResVar(), rightCode.getResVar(), cmpOp, leftCode.getResVar(), rightCode.getResVar()));

        return code;
    }

    @Override
    public Class<?> getDataType()
    {
        return Boolean.class;
    }

    private Object evalInternal(Object leftResult, Object rightResult)
    {
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
    public String toString()
    {
        return left.toString() + " " + type.value + " " + right.toString();
    }

    @Override
    public int hashCode()
    {
        //CSOFF
        int hashCode = 17;
        hashCode = hashCode * 37 + left.hashCode();
        hashCode = hashCode * 37 + right.hashCode();
        return hashCode;
        //CSON
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ComparisonExpression)
        {
            ComparisonExpression e = (ComparisonExpression) obj;
            return type.equals(e.type)
                &&
                left.equals(e.left)
                &&
                right.equals(e.right);
        }
        return false;
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    /** Type */
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
