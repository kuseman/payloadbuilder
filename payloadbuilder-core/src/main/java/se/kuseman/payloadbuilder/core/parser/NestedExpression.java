package se.kuseman.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.codegen.CodeGeneratorContext;
import se.kuseman.payloadbuilder.api.codegen.ExpressionCode;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Nested expression */
public class NestedExpression extends Expression
{
    private final Expression expression;

    public NestedExpression(Expression expression)
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
        // Only fold nested expressions to remove excessive parenthesis
        if (expression instanceof NestedExpression)
        {
            return expression.fold();
        }
        return expression;
    }

    @Override
    public DataType getDataType()
    {
        return expression.getDataType();
    }

    @Override
    public Object eval(IExecutionContext context)
    {
        return expression.eval(context);
    }

    @Override
    public boolean isCodeGenSupported()
    {
        return expression.isCodeGenSupported();
    }

    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context)
    {
        return expression.generateCode(context);
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
        if (obj instanceof NestedExpression)
        {
            return expression.equals(((NestedExpression) obj).expression);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "(" + expression.toString() + ")";
    }
}
