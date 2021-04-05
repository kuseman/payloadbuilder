package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;

/** Expression for is null / is not null */
public class NullPredicateExpression extends Expression
{
    private final Expression expression;
    private final boolean not;

    public NullPredicateExpression(Expression expression, boolean not)
    {
        this.expression = requireNonNull(expression);
        this.not = not;
    }

    public Expression getExpression()
    {
        return expression;
    }

    public boolean isNot()
    {
        return not;
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
            boolean nullValue = expression instanceof LiteralNullExpression;
            if (not)
            {
                nullValue = !nullValue;
            }

            return nullValue
                ? LiteralBooleanExpression.TRUE_LITERAL
                : LiteralBooleanExpression.FALSE_LITERAL;
        }

        return this;
    }

    @Override
    public Object eval(ExecutionContext context)
    {
        Object result = expression.eval(context);
        return not ? result != null : result == null;
    }

    @Override
    public boolean isCodeGenSupported()
    {
        return expression.isCodeGenSupported();
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context)
    {
        ExpressionCode code = context.getCode();
        ExpressionCode childCode = expression.generateCode(context);

        /*
        * Boolean res0 = false;
        * Object res1 = res0 == null / res0 != null
        *
        */

        String template = "//%s\n%s"
            + "Boolean %s = %s %s= null;\n";

        code.setCode(String.format(template,
                toString(),
                childCode.getCode(),
                code.getResVar(), childCode.getResVar(), not ? "!" : "="));

        return code;
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public Class<?> getDataType()
    {
        return boolean.class;
    }

    @Override
    public String toString()
    {
        return expression + " IS " + (not ? " NOT " : "") + "NULL";
    }

    @Override
    public int hashCode()
    {
        return expression.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof NullPredicateExpression)
        {
            NullPredicateExpression that = (NullPredicateExpression) obj;
            return expression.equals(that.expression)
                && not == that.not;
        }
        return false;
    }
}
