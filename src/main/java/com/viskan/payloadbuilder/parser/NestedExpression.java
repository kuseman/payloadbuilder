package com.viskan.payloadbuilder.parser;

import com.viskan.payloadbuilder.codegen.CodeGeneratorContext;
import com.viskan.payloadbuilder.codegen.ExpressionCode;

import static java.util.Objects.requireNonNull;

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
        return expression.fold();
    }
    
    @Override
    public boolean isNullable()
    {
        return expression.isNullable();
    }
    
    @Override
    public Class<?> getDataType()
    {
        return expression.getDataType();
    }
    
    @Override
    public Object eval(ExecutionContext context)
    {
        return expression.eval(context);
    }
    
    @Override
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
    {
        return expression.generateCode(context, parentCode);
    }
    
    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }
    
    @Override
    public int hashCode()
    {
        return 37 * expression.hashCode();
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
