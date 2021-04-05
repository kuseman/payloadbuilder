package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.utils.ExpressionMath;

/** Arithmetic unary expression */
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
    public Class<?> getDataType()
    {
        return expression.getDataType();
    }

    @Override
    public boolean isConstant()
    {
        return expression.isConstant();
    }

    @Override
    public Expression fold()
    {
        if (expression instanceof LiteralExpression)
        {
            Object result = ((LiteralExpression) expression).getObjectValue();
            return result == null
                ? LiteralNullExpression.NULL_LITERAL
                : LiteralExpression.create(ExpressionMath.negate(result));
        }

        return this;
    }

    @Override
    public Object eval(ExecutionContext context)
    {
        Object result = expression.eval(context);
        return result != null ? ExpressionMath.negate(result) : null;
    }

    //    @Override
    //    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
    //    {
    //        ExpressionCode childCode = expression.generateCode(context, parentCode);
    //        ExpressionCode code = ExpressionCode.code(context);
    //
    //        String method = null;
    //        //CSOFF
    //        switch (type)
    //        //CSON
    //        {
    //            case MINUS:
    //                method = "ExpressionMath.negate";
    //                break;
    //            case PLUS:
    //                throw new NotImplementedException("unary plus");
    //        }
    //
    //        String template = "%s"
    //            + "Object %s = null;\n"
    //            + "boolean %s = true;\n"
    //            + "if (!%s)\n"
    //            + "{\n"
    //            + "  %s = %s(%s);\n"
    //            + "  %s = false;\n"
    //            + "}\n";
    //
    //        code.setCode(String.format(template,
    //                childCode.getCode(),
    //                code.getResVar(),
    //                code.getIsNull(),
    //                childCode.getIsNull(),
    //                code.getResVar(), method, childCode.getResVar(),
    //                code.getIsNull()));
    //
    //        return code;
    //    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public int hashCode()
    {
        return expression.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ArithmeticUnaryExpression)
        {
            ArithmeticUnaryExpression that = (ArithmeticUnaryExpression) obj;
            return expression.equals(that.expression)
                && type == that.type;
        }
        return false;
    }

    /** Type */
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
