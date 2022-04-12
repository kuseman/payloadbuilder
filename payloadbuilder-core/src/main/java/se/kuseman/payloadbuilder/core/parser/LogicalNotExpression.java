package se.kuseman.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;
import static se.kuseman.payloadbuilder.core.parser.LiteralBooleanExpression.FALSE_LITERAL;
import static se.kuseman.payloadbuilder.core.parser.LiteralBooleanExpression.TRUE_LITERAL;
import static se.kuseman.payloadbuilder.core.parser.LiteralNullExpression.NULL_LITERAL;

import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.codegen.CodeGeneratorContext;
import se.kuseman.payloadbuilder.api.codegen.ExpressionCode;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Logical not */
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
    public boolean isConstant()
    {
        return expression.isConstant();
    }

    @Override
    public Expression fold()
    {
        if (expression instanceof LiteralNullExpression)
        {
            return NULL_LITERAL;
        }
        else if (expression instanceof LiteralBooleanExpression)
        {
            boolean value = ((LiteralBooleanExpression) expression).getValue();
            return value ? FALSE_LITERAL
                    : TRUE_LITERAL;
        }
        else if (expression instanceof InExpression)
        {
            InExpression inExpression = (InExpression) expression;
            return new InExpression(inExpression.getExpression(), inExpression.getArguments(), !inExpression.isNot());
        }
        else if (expression instanceof LikeExpression)
        {
            LikeExpression likeExpression = (LikeExpression) expression;
            return new LikeExpression(likeExpression.getExpression(), likeExpression.getPatternExpression(), !likeExpression.isNot(), likeExpression.getEscapeCharacterExpression());
        }

        return this;
    }

    @Override
    public DataType getDataType()
    {
        return expression.getDataType();
    }

    @Override
    public Object eval(IExecutionContext context)
    {
        Object result = expression.eval(context);
        return result != null ? !(Boolean) result
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

        /*
         * Object v1 = null; v1 = !n1 ? !(Boolean) v1 : f
         */

        String template = "// NOT\n" + "%s" // childCode
                          + "if (!%s) %s = !%s%s;\n"; // child nullVar, child resVar, cast, child resVar

        childCode.setCode(String.format(template, childCode.getCode(), childCode.getNullVar(), childCode.getResVar(), expression.getDataType() != DataType.BOOLEAN ? "(Boolean)"
                : "", childCode.getResVar()));

        return childCode;
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
        if (obj instanceof LogicalNotExpression)
        {
            return expression.equals(((LogicalNotExpression) obj).expression);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "NOT " + expression.toString();
    }
}
