package se.kuseman.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.codegen.CodeGeneratorContext;
import se.kuseman.payloadbuilder.api.codegen.ExpressionCode;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.utils.ExpressionMath;

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
    public DataType getDataType()
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
            return result == null ? LiteralNullExpression.NULL_LITERAL
                    : LiteralExpression.create(ExpressionMath.negate(result));
        }

        return this;
    }

    @Override
    public Object eval(IExecutionContext context)
    {
        Object result = expression.eval(context);
        return result != null ? ExpressionMath.negate(result)
                : null;
    }

    @Override
    public boolean isCodeGenSupported()
    {
        return expression.isCodeGenSupported();
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context)
    {
        ExpressionCode childCode = expression.generateCode(context);
        ExpressionCode code = context.getExpressionCode();

        DataType dataType = getDataType();
        if (dataType == DataType.ANY)
        {
            context.addImport("se.kuseman.payloadbuilder.api.utils.ExpressionMath");
            String method = null;
            // CSOFF
            switch (type)
            // CSON
            {
                case MINUS:
                    method = "ExpressionMath.negate";
                    break;
                case PLUS:
                    throw new IllegalArgumentException("unary plus");
            }

            String template = "%s" // childCode
                              + "Object %s = null;\n" // resVar
                              + "boolean %s = true;\n" // nullVar
                              + "if (!%s)\n" // childCode nullVar
                              + "{\n"
                              + "  %s = %s(%s);\n" // resVar, method, childCode resVar
                              + "  %s = false;\n" // nullVar
                              + "}\n";

            code.setCode(String.format(template, childCode.getCode(), code.getResVar(), code.getNullVar(), childCode.getNullVar(), code.getResVar(), method, childCode.getResVar(), code.getNullVar()));
        }
        else
        {
            String template = "%s" // childCode
                              + "%s %s = %s;\n" // dataType resVar default
                              + "boolean %s = %s;\n" // nullVar, childCode nullVar
                              + "if (!%s)\n" // childCode nullVar
                              + "{\n"
                              + "  %s = -%s;\n" // resVar, childCode resVar
                              + "}\n";

            code.setCode(String.format(template, childCode.getCode(), context.getJavaTypeString(dataType), code.getResVar(), context.getJavaDefaultValue(dataType), code.getNullVar(),
                    childCode.getNullVar(), childCode.getNullVar(), code.getResVar(), childCode.getResVar()));
        }

        return code;
    }

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
