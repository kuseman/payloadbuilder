package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;
import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.FALSE_LITERAL;
import static org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression.TRUE_LITERAL;
import static org.kuse.payloadbuilder.core.parser.LiteralNullExpression.NULL_LITERAL;

import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;

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
            return value ? FALSE_LITERAL : TRUE_LITERAL;
        }

        return this;
    }

    @Override
    public Class<?> getDataType()
    {
        return expression.getDataType();
    }

    @Override
    public Object eval(ExecutionContext context)
    {
        Object result = expression.eval(context);
        return result != null ? !(Boolean) result : null;
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
         * Object v1 = null;
         * v1 = v1 != null ? !(Boolean) v1 : false;
         *
         */

        String template = "// NOT\n%s\n"
            + "%s = %s != null ? !(Boolean)%s : null;\n";

        childCode.setCode(String.format(template,
                childCode.getCode(),
                childCode.getResVar(), childCode.getResVar(), childCode.getResVar()));

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
